from qcloud_cos import CosConfig
from qcloud_cos import CosS3Client, CosServiceError
import sys
import logging
import sys
import importlib.abc
import imp
from importlib.machinery import SOURCE_SUFFIXES, BYTECODE_SUFFIXES, EXTENSION_SUFFIXES
from importlib._bootstrap_external import _validate_bytecode_header, _compile_bytecode
from importlib._bootstrap_external import _bootstrap, _imp

__version__ = '0.0.1'

logging.basicConfig(
    # level=logging.DEBUG,
    format = '%(asctime)s - %(name)s - %(filename)s[%(lineno)d] - %(levelname)s - %(message)s',
    stream=sys.stdout)
log = logging.getLogger(__name__)
# log.setLevel(logging.DEBUG)

_cosclient = None
_cosbucket = None

def _install_cos(bucket, secret_id, secret_key, region, token=''):
    global _cosclient, _cosbucket
    config = CosConfig(Secret_id=secret_id, Secret_key=secret_key, Region=region, Token=token)
    _cosclient = CosS3Client(config)
    _cosbucket = bucket
    install_path_hook()

def _format_path(path):
    return path.replace('\\', '/').strip('/')

def _path_join(*paths):
    return '/'.join(map(_format_path, paths))

_links = {}

def _get_links(key):
    global _cosclient, _cosbucket, _links
    key = _format_path(key) + '/'
    if key.startswith('cos://'):
        key = key[6:]
    if key in _links:
        return _links[key]
    log.debug('get links key: %s', key)
    ret = _cosclient.list_objects(_cosbucket, key, Delimiter="/", MaxKeys=100)
    contents = ret.get('Contents', set())
    commonPrefixes = ret.get('CommonPrefixes', set())
    l = len(key)
    links = {x['Key'][l:] for x in contents if x['Key']!=key}
    links |= {x['Prefix'][l:].rstrip('/') for x in commonPrefixes}
    log.debug('links: %s', links)
    _links[key] = links
    return links

class CosSourceLoader(importlib.abc.SourceLoader):
    def __init__(self, path):
        self._basepath = path
        self._source_cache = {}
        self._data_cache = {}

    def module_repr(self, module):
        return '<cosmodule %r from %r>' % (module.__name__, module.__file__)

    # def load_module(self, fullname): # 这个方法不能解决包内相对导入问题， 不实现使用默认方法
    #     log.debug('load_module: %s', fullname)
    #     return _load_module_shim(self, fullname)
    #     code = self.get_code(fullname)
    #     mod = sys.modules.setdefault(fullname, imp.new_module(fullname))
    #     mod.__name__ = fullname
    #     mod.__file__ = self.get_filename(fullname)
    #     mod.__loader__ = self
    #     log.debug('mod attr:%s', mod.__dict__)
    #     exec(code, mod.__dict__)
    #     return mod

    # def get_code(self, fullname):
    #     print('############## fullname:', fullname)
    #     src = self.get_source(fullname)
    #     return compile(src, self.get_filename(fullname), 'exec', dont_inherit=True)

    def get_data(self, path):
        path = _format_path(path)[6:]
        log.debug('loader: reading %r', path)
        if path in self._data_cache:
            log.debug('loader: cached %r', path)
            return self._data_cache[path]
        try:
            u = _cosclient.get_object(_cosbucket, path)
            data = u['Body'].get_raw_stream().read()
            log.debug('loade data: %r loaded', path)
            self._data_cache[path] = data
            return data
        except CosServiceError as e:
            log.debug('loade data: %r failed. %s', path, e)
            raise ImportError("Can't load data %s" % path)

    def get_filename(self, fullname):
        links = _get_links(self._basepath)
        basename = fullname.split('.')[-1]
        for ext in SOURCE_SUFFIXES:
            filename = basename + ext
            if filename in links:
                filename = _path_join(self._basepath, filename)
                if not filename.startswith('cos://'):
                    filename = 'cos://' + filename
                return filename

    def get_source(self, fullname):
        filename = self.get_filename(fullname)
        source = self.get_data(filename).decode('utf-8')
        return source

    def is_package(self, fullname):
        return False

class CosSourcelessLoader(CosSourceLoader):
    def get_code(self, fullname):
        path = self.get_filename(fullname)
        data = self.get_data(path)
        bytes_data = _validate_bytecode_header(data, name=fullname, path=path)
        return _compile_bytecode(bytes_data, name=fullname, bytecode_path=path)


    def get_filename(self, fullname):
        links = _get_links(self._basepath)
        basename = fullname.split('.')[-1]
        for ext in BYTECODE_SUFFIXES:
            filename = basename + ext
            if filename in links:
                filename = _path_join(self._basepath, filename)
                if not filename.startswith('cos://'):
                    filename = 'cos://' + filename
                return filename

    def get_source(self, fullname):
        pass

class CosExtensionLoader(CosSourceLoader):
    def get_code(self, fullname):
        pass

    def get_source(self, fullname):
        pass

    def get_filename(self, fullname):
        links = _get_links(self._basepath)
        basename = fullname.split('.')[-1]
        for ext in EXTENSION_SUFFIXES:
            filename = basename + ext
            if filename in links:
                filename = _path_join(self._basepath, filename)
                if not filename.startswith('cos://'):
                    filename = 'cos://' + filename
                return filename

    def create_module(self, spec):
        """Create an unitialized extension module"""
        module = _bootstrap._call_with_frames_removed(
            _imp.create_dynamic, spec)
        return module

    def exec_module(self, module):
        """Initialize an extension module"""
        _bootstrap._call_with_frames_removed(_imp.exec_dynamic, module)


class CosPackageLoader(CosSourceLoader):
    def load_module(self, fullname):
        mod = super().load_module(fullname)
        mod.__path__ = [ self._basepath ]
        mod.__package__ = fullname

    def get_filename(self, fullname):
        filename = _path_join(self._basepath, '__init__.py')
        if not filename.startswith('cos://'):
            filename = 'cos://' + filename
        return filename

    def is_package(self, fullname):
        return True

class CosPathFinder(importlib.abc.PathEntryFinder):
    def __init__(self, baseurl):
        self._links = None
        # self._loader = CosSourceLoader(baseurl)
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
            fullurl = _path_join(self._baseurl, basename)
            if not fullurl.startswith('cos://'):
                fullurl = 'cos://' + fullurl
            # Attempt to load the package (which accesses __init__.py)
            loader = CosPackageLoader(fullurl)
            try:
                loader.load_module(fullname)
                log.debug('find_loader: package %r loaded', fullname)
            except ImportError as e:
                import traceback as tb
                log.debug(tb.format_exc())
                log.debug('find_loader: %r is a namespace package', fullname)
                loader = None
            return (loader, [fullurl])
        # A normal module
        exts = [SOURCE_SUFFIXES, BYTECODE_SUFFIXES, EXTENSION_SUFFIXES]
        clses = [CosSourceLoader, CosSourcelessLoader, CosExtensionLoader]
        for ext, cls in zip(exts, clses):
            for ex in ext:
                filename = basename + ex
                if filename in self._links:
                    log.debug('find_loader: module %r found', fullname)
                    return (cls(self._baseurl), [])
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