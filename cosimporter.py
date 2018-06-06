from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client, CosServiceError
import sys
import logging
import sys
import importlib.abc
import imp

logging.basicConfig(level=logging.WARN, stream=sys.stdout)
log = logging.getLogger(__name__)

_cosclient = None
_cosbucket = None

def _install_cos(bucket, secret_id, secret_key, region, token=''):
    global _cosclient, _cosbucket
    config = CosConfig(Secret_id=secret_id, Secret_key=secret_key, Region=region, Token=token)
    _cosclient = CosS3Client(config)
    _cosbucket = bucket
    install_path_hook()

def _get_links(key):
    global _cosclient, _cosbucket
    # if key.startswith('cos://'):
    #     key = key[6:]
    key = key.replace('//', '/')
    key = key.strip('/') + '/'
    ret = _cosclient.list_objects(_cosbucket, key, Delimiter="/", MaxKeys=100)
    log.debug('ret: %s', ret)
    contents = ret.get('Contents')
    commonPrefixes = ret.get('CommonPrefixes')
    if contents is None:
        return set()
    l = len(key)
    links = {x['Key'][l:] for x in contents if x['Key']!=key}
    if commonPrefixes is not None:
        links |= {x['Prefix'][l:].rstrip('/') for x in commonPrefixes}
    log.debug('links: %s', links)
    return links

class CosMetaFinder(importlib.abc.MetaPathFinder):
    def __init__(self, key):
        global _cosclient
        if _cosclient is None:
            return
        self._basepath = key
        self._links = {}
        self._loaders = {key: CosModuleLoader(key)}

    def find_module(self, fullname, path=None):  #根据fullname返回moduleloader
        log.debug('find_module: fullname=%r, path=%r', fullname, path)
        global _cosclient
        if _cosclient is None:
            return None
        if path is None:
            baseurl = self._basepath
        else:
            if not path[0].startswith('cos://'+self._basepath):
                return None
            baseurl = path[0][6:]
        parts = fullname.split('.')
        basename = parts[-1]
        log.debug('find_module: baseurl=%r, basename=%r', baseurl, basename)
        if basename not in self._links:
            self._links[baseurl] = _get_links(baseurl)
        if basename in self._links[baseurl]:
            log.debug('find_module: trying package %r', fullname)
            fullurl = self._basepath + '/' + basename
            fullurl = fullurl.replace('//', '/')
            loader = CosPackageLoader(fullurl)
            try:
                loader.load_module(fullname)
                self._links[fullurl] = _get_links(fullurl)
                self._loaders[fullurl] = CosModuleLoader(fullurl)
                log.debug('find_module: package %r loaded', fullname)
            except ImportError as e:
                log.debug('find_module: package failed. %s', e)
                loader = None
            return loader
        # A normal module
        filename = basename + '.py'
        if filename in self._links[baseurl]:
            log.debug('find_module: module %r found', fullname)
            return self._loaders[baseurl]
        else:
            log.debug('find_module: module %r not found', fullname)
            return None

    def invalidate_caches(self):
        log.debug('invalidating link cache')
        self._links.clear()


class CosModuleLoader(importlib.abc.SourceLoader):
    def __init__(self, path):
        self._basepath = path
        self._source_cache = {}

    def module_repr(self, module):
        return '<cosmodule %r from %r>' % (module.__name__, module.__file__)

    def load_module(self, fullname): #返回模块
        code = self.get_code(fullname)
        mod = sys.modules.setdefault(fullname, imp.new_module(fullname))
        mod.__file__ = self.get_filename(fullname)
        mod.__loader__ = self
        mod.__package__ = fullname.rpartition('.')[0]
        exec(code, mod.__dict__)
        return mod

    def get_code(self, fullname):
        src = self.get_source(fullname)
        return compile(src, self.get_filename(fullname), 'exec')

    def get_data(self, path):
        pass

    def get_filename(self, fullname):
        return 'cos://'+(self._basepath + '/' + fullname.split('.')[-1] + '.py').replace('//', '/')

    def get_source(self, fullname):
        global _cosclient, _cosbucket
        filename = self.get_filename(fullname)
        filename = filename[6:]
        log.debug('loader: reading %r', filename)
        if filename in self._source_cache:
            log.debug('loader: cached %r', filename)
            return self._source_cache[filename]
        try:
            u = _cosclient.get_object(_cosbucket, filename)
            source = u['Body'].get_raw_stream().read().decode('utf-8')
            log.debug('loader: %r loaded', filename)
            self._source_cache[filename] = source
            return source
        except CosServiceError as e:
            log.debug('loader: %r failed. %s', filename, e)
            raise ImportError("Can't load %s" % filename)

    def is_package(self, fullname):
        return False

class CosPackageLoader(CosModuleLoader):
    def load_module(self, fullname):
        mod = super().load_module(fullname)
        mod.__path__ = [ self._basepath ]
        mod.__package__ = fullname

    def get_filename(self, fullname):
        return 'cos://' + (self._basepath + '/' + '__init__.py').replace('//', '/')

    def is_package(self, fullname):
        return True

# Utility functions for installing/uninstalling the loader
_installed_meta_cache = { }
def install_meta(address):
    if address not in _installed_meta_cache:
        finder = CosMetaFinder(address)
        _installed_meta_cache[address] = finder
        sys.meta_path.append(finder)
        log.debug('%r installed on sys.meta_path', finder)

def remove_meta(address):
    if address in _installed_meta_cache:
        finder = _installed_meta_cache.pop(address)
        sys.meta_path.remove(finder)
        logging.debug('%r removed from sys.meta_path', finder)

class CosPathFinder(importlib.abc.PathEntryFinder):
    def __init__(self, baseurl):
        self._links = None
        self._loader = CosModuleLoader(baseurl)
        self._baseurl = baseurl

    def find_loader(self, fullname):
        log.debug('find_loader: %r', fullname)
        parts = fullname.split('.')
        basename = parts[-1]
        # Check link cache
        if self._links is None:
            self._links = [] # See discussion
            self._links = _get_links(self._baseurl)

        # Check if it's a package
        if basename in self._links:
            log.debug('find_loader: trying package %r', fullname)
            fullurl = (self._baseurl + '/' + basename).replace('//', '/')
            # Attempt to load the package (which accesses __init__.py)
            loader = CosPackageLoader(fullurl)
            try:
                loader.load_module(fullname)
                log.debug('find_loader: package %r loaded', fullname)
            except ImportError as e:
                log.debug('find_loader: %r is a namespace package', fullname)
                loader = None
            return (loader, [fullurl])

        # A normal module
        filename = basename + '.py'
        if filename in self._links:
            log.debug('find_loader: module %r found', fullname)
            return (self._loader, [])
        else:
            log.debug('find_loader: module %r not found', fullname)
            return (None, [])

    def invalidate_caches(self):
        log.debug('invalidating link cache')
        self._links = None

_url_path_cache = {}

def handle_url(path):
    if path.startswith('cos://'):
        log.debug('Handle path? %s. [Yes]', path)
        path = path[6:]
        if path in _url_path_cache:
            finder = _url_path_cache[path]
        else:
            finder = CosPathFinder(path)
            _url_path_cache[path] = finder
        return finder
    else:
        log.debug('Handle path? %s. [No]', path)

def install_path_hook():
    sys.path_hooks.append(handle_url)
    sys.path_importer_cache.clear()
    log.debug('Installing handle_url')

def remove_path_hook():
    sys.path_hooks.remove(handle_url)
    sys.path_importer_cache.clear()
    log.debug('Removing handle_url')

if __name__ == '__main__':
    pass