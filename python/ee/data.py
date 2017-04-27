#!/usr/bin/env python
"""Singleton for the library's communication with the Earth Engine API."""

from __future__ import print_function

# pylint: disable=g-bad-import-order
import contextlib
import json
from threading import local

import httplib2
import six
import time

# Using lowercase function naming to match the JavaScript names.
# pylint: disable=g-bad-name

# pylint: disable=g-import-not-at-top
try:
  # Python 3.x
  import urllib.error
  import urllib.parse
  import urllib.request
except ImportError:
  # Python 2.x
  import urllib

from . import ee_exception

# The HTTP header through which profile results are returned.
# Lowercase because that's how httplib2 does things.
_PROFILE_HEADER_LOWERCASE = 'x-earth-engine-computation-profile'

# Maximum number of times to retry a rate-limited request.
MAX_RETRIES = 5

# Maximum time to wait before retrying a rate-limited request (in milliseconds).
MAX_RETRY_WAIT = 120000

# Base time (in ms) to wait when performing exponential backoff in request
# retries.
BASE_RETRY_WAIT = 1000

# The default base URL for API calls.
DEFAULT_API_BASE_URL = 'https://earthengine.googleapis.com/api'

# The default base URL for media/tile calls.
DEFAULT_TILE_BASE_URL = 'https://earthengine.googleapis.com'

# Asset types recognized by create_assets().
ASSET_TYPE_FOLDER = 'Folder'
ASSET_TYPE_IMAGE_COLL = 'ImageCollection'

_LOCAL = local()

_global_data = None

def initialize(credentials=None, api_base_url=None, tile_base_url=None):
  """Initializes the data module, setting credentials and base URLs.

  If any of the arguments are unspecified, they will keep their old values;
  the defaults if initialize() has never been called before.

  Args:
    credentials: The OAuth2 credentials.
    api_base_url: The EarthEngine REST API endpoint.
    tile_base_url: The EarthEngine REST tile endpoint.
  """
  return _instance().initialize(credentials, api_base_url, tile_base_url)


def reset():
  """Resets the data module, clearing credentials and custom base URLs."""
  return _instance().reset()


def setDeadline(milliseconds):
  """Sets the timeout length for API requests.

  Args:
    milliseconds: The number of milliseconds to wait for a request
        before considering it timed out. 0 means no limit.
  """
  return _instance().setDeadline(milliseconds)


@contextlib.contextmanager
def profiling(hook):
  # pylint: disable=g-doc-return-or-yield
  """Returns a context manager which enables or disables profiling.

  If hook is not None, enables profiling for all API calls in its scope and
  calls the hook function with all resulting profile IDs. If hook is null,
  disables profiling (or leaves it disabled).

  Args:
    hook: A function of one argument which is called with each profile
        ID obtained from API calls, just before the API call returns.
  """
  return _instance().profiling(hook)


def getInfo(asset_id):
  """Load info for an asset, given an asset id.

  Args:
    asset_id: The asset to be retrieved.

  Returns:
    The value call results.
  """
  return _instance().getInfo(asset_id)


def getList(params):
  """Get a list of contents for a collection asset.

  Args:
    params: An object containing request parameters with the
        following possible values:
            id (string) The asset id of the collection to list.
            starttime (number) Start time, in msec since the epoch.
            endtime (number) End time, in msec since the epoch.
            fields (comma-separated strings) Field names to return.

  Returns:
    The list call results.
  """
  return _instance().getList(params)


def getMapId(params):
  """Get a Map ID for a given asset.

  Args:
    params: An object containing visualization options with the
            following possible values:
      image - (JSON string) The image to render.
      version - (number) Version number of image (or latest).
      bands - (comma-seprated strings) Comma-delimited list of
          band names to be mapped to RGB.
      min - (comma-separated numbers) Value (or one per band)
          to map onto 00.
      max - (comma-separated numbers) Value (or one per band)
          to map onto FF.
      gain - (comma-separated numbers) Gain (or one per band)
          to map onto 00-FF.
      bias - (comma-separated numbers) Offset (or one per band)
          to map onto 00-FF.
      gamma - (comma-separated numbers) Gamma correction
          factor (or one per band)
      palette - (comma-separated strings) A string of comma-separated
          CSS-style color strings (single-band previews only). For example,
          'FF0000,000000'.
      format (string) Either 'jpg' (does not support transparency) or
          'png' (supports transparency).

  Returns:
    A dictionary containing "mapid" and "token" strings, which can
    be combined to retrieve tiles from the /map service.
  """
  return _instance().getMapId(params)


def getTileUrl(mapid, x, y, z):
  """Generate a URL for map tiles from a Map ID and coordinates.

  Args:
    mapid: The Map ID to generate tiles for, a dictionary containing "mapid"
        and "token" strings.
    x: The tile x coordinate.
    y: The tile y coordinate.
    z: The tile zoom level.

  Returns:
    The tile URL.
  """
  return _instance().getTileUrl(mapid, x, y, z)


def getValue(params):
  """Retrieve a processed value from the front end.

  Args:
    params: A dictionary containing:
        json - (String) A JSON object to be evaluated.

  Returns:
    The value call results.
  """
  return _instance().getValue(params)


def getThumbnail(params):
  """Get a Thumbnail for a given asset.

  Args:
    params: Parameters identical to getMapId, plus:
        size - (a number or pair of numbers in format WIDTHxHEIGHT) Maximum
          dimensions of the thumbnail to render, in pixels. If only one number
          is passed, it is used as the maximum, and the other dimension is
          computed by proportional scaling.
        region - (E,S,W,N or GeoJSON) Geospatial region of the image
          to render. By default, the whole image.
        format - (string) Either 'png' (default) or 'jpg'.

  Returns:
    A thumbnail image as raw PNG data.
  """
  return _instance().getThumbnail(params)


def getThumbId(params):
  """Get a Thumbnail ID for a given asset.

  Args:
    params: Parameters identical to getMapId, plus:
        size - (a number or pair of numbers in format WIDTHxHEIGHT) Maximum
          dimensions of the thumbnail to render, in pixels. If only one number
          is passed, it is used as the maximum, and the other dimension is
          computed by proportional scaling.
        region - (E,S,W,N or GeoJSON) Geospatial region of the image
          to render. By default, the whole image.
        format - (string) Either 'png' (default) or 'jpg'.

  Returns:
    A thumbnail ID.
  """
  return _instance().getThumbId(params)


def makeThumbUrl(thumbId):
  """Create a thumbnail URL from the given thumbid and token.

  Args:
    thumbId: An object containing a thumbnail thumbid and token.

  Returns:
    A URL from which the thumbnail can be obtained.
  """
  return _instance().makeThumbUrl(thumbId)


def getDownloadId(params):
  """Get a Download ID.

  Args:
    params: An object containing visualization options with the following
      possible values:
        name - a base name to use when constructing filenames.
        bands - a description of the bands to download. Must be an array of
            dictionaries, each with the following keys:
          id - the name of the band, a string, required.
          crs - an optional CRS string defining the band projection.
          crs_transform - an optional array of 6 numbers specifying an affine
              transform from the specified CRS, in the order: xScale,
              yShearing, xShearing, yScale, xTranslation and yTranslation.
          dimensions - an optional array of two integers defining the width and
              height to which the band is cropped.
          scale - an optional number, specifying the scale in meters of the
                 band; ignored if crs and crs_transform is specified.
        crs - a default CRS string to use for any bands that do not explicitly
            specify one.
        crs_transform - a default affine transform to use for any bands that do
            not specify one, of the same format as the crs_transform of bands.
        dimensions - default image cropping dimensions to use for any bands
            that do not specify them.
        scale - a default scale to use for any bands that do not specify one;
            ignored if crs and crs_transform is specified.
        region - a polygon specifying a region to download; ignored if crs
            and crs_transform is specified.

  Returns:
    A dict containing a docid and token.
  """
  return _instance().getDownloadId(params)


def makeDownloadUrl(downloadId):
  """Create a download URL from the given docid and token.

  Args:
    downloadId: An object containing a download docid and token.

  Returns:
    A URL from which the download can be obtained.
  """
  return _instance().makeDownloadUrl(downloadId)


def getTableDownloadId(params):
  """Get a Download ID.

  Args:
    params: An object containing table download options with the following
      possible values:
        format - The download format, CSV or JSON.
        selectors - Comma separated string of selectors that can be used to
            determine which attributes will be downloaded.
        filename - The name of the file that will be downloaded.

  Returns:
    A dict containing a docid and token.
  """
  return _instance().getTableDownloadId(params)


def makeTableDownloadUrl(downloadId):
  """Create a table download URL from a docid and token.

  Args:
    downloadId: A table download id and token.

  Returns:
    A Url from which the download can be obtained.
  """
  return _instance().makeTableDownloadUrl(downloadId)


def getAlgorithms():
  """Get the list of algorithms.

  Returns:
    The dictionary of algorithms.  Each algorithm is a dictionary containing
    the following fields:
        "description" - (string) A text description of the algorithm.
        "returns" - (string) The return type of the algorithm.
        "args" - An array of arguments.  Each argument specifies the following:
            "name" - (string) The name of the argument.
            "description" - (string) A text description of the argument.
            "type" - (string) The type of the argument.
            "optional" - (boolean) Whether the argument is optional or not.
            "default" - A representation of the default value if the argument
                is not specified.
  """
  return _instance().getAlgorithms()


def createAsset(value, opt_path=None):
  """Creates an asset from a JSON value.

  To create an empty image collection or folder, pass in a "value" object
  with a "type" key whose value is "ImageCollection" or "Folder".

  Args:
    value: An object describing the asset to create or a JSON string
        with the already-serialized value for the new asset.
    opt_path: An optional desired ID, including full path.

  Returns:
    A description of the saved asset, including a generated ID.
  """
  return _instance().createAsset(value, opt_path)


def copyAsset(sourceId, destinationId):
  """Copies the asset from sourceId into destinationId.

  Args:
    sourceId: The ID of the asset to copy.
    destinationId: The ID of the new asset created by copying.
  """
  return _instance().copyAsset(sourceId, destinationId)


def renameAsset(sourceId, destinationId):
  """Renames the asset from sourceId to destinationId.

  Args:
    sourceId: The ID of the asset to rename.
    destinationId: The new ID of the asset.
  """
  return _instance().renameAsset(sourceId, destinationId)


def deleteAsset(assetId):
  """Deletes the asset with the given id.

  Args:
    assetId: The ID of the asset to delete.
  """
  return _instance().deleteAsset(assetId)


def newTaskId(count=1):
  """Generate an ID for a long-running task.

  Args:
    count: Optional count of IDs to generate, one by default.

  Returns:
    A list containing generated ID strings.
  """
  return _instance().newTaskId(count)


def getTaskList():
  """Retrieves a list of the user's tasks.

  Returns:
    A list of task status dictionaries, one for each task submitted to EE by
    the current user. These include currently running tasks as well as recently
    canceled or failed tasks.
  """
  return _instance().getTaskList()


def getTaskStatus(taskId):
  """Retrieve status of one or more long-running tasks.

  Args:
    taskId: ID of the task or a list of multiple IDs.

  Returns:
    List containing one object for each queried task, in the same order as
    the input array, each object containing the following values:
      id (string) ID of the task.
      state (string) State of the task, one of READY, RUNNING, COMPLETED,
        FAILED, CANCELLED; or UNKNOWN if the task with the specified ID
        doesn't exist.
      error_message (string) For a FAILED task, a description of the error.
  """
  return _instance().getTaskStatus(taskId)


def cancelTask(taskId):
  return _instance().cancelTask(taskId)


def startProcessing(taskId, params):
  """Create processing task that exports or pre-renders an image.

  Args:
    taskId: ID for the task (obtained using newTaskId).
    params: The object that describes the processing task; only fields
      that are common for all processing types are documented below.
        type (string) Either 'EXPORT_IMAGE', 'EXPORT_FEATURES',
          'EXPORT_VIDEO', or 'EXPORT_TILES'.
        json (string) JSON description of the image.

  Returns:
    A dict with optional notes about the created task.
  """
  return _instance().startProcessing(taskId, params)


def startIngestion(taskId, params):
  """Creates an asset import task.

  Args:
    taskId: ID for the task (obtained using newTaskId).
    params: The object that describes the import task, which can
        have these fields:
          id (string) The destination asset id (e.g. users/foo/bar).
          tilesets (array) A list of Google Cloud Storage source file paths
            formatted like:
              [{'sources': [
                  {'primaryPath': 'foo.tif', 'additionalPaths': ['foo.prj']},
                  {'primaryPath': 'bar.tif', 'additionalPaths': ['bar.prj'},
              ]}]
            Where path values correspond to source files' Google Cloud Storage
            object names, e.g. 'gs://bucketname/filename.tif'
          bands (array) An optional list of band names formatted like:
            [{'id': 'R'}, {'id': 'G'}, {'id': 'B'}]

  Returns:
    A dict with optional notes about the created task.
  """
  return _instance().startIngestion(taskId, params)


def getAssetRoots():
  """Returns the list of the root folders the user owns.

  Note: The "id" values for roots are two levels deep, e.g. "users/johndoe"
        not "users/johndoe/notaroot".

  Returns:
    A list of folder descriptions formatted like:
      [
          {"type": "Folder", "id": "users/foo"},
          {"type": "Folder", "id": "projects/bar"},
      ]
  """
  return _instance().getAssetRoots()


def getAssetRootQuota(rootId):
  """Returns quota usage details for the asset root with the given ID.

  Usage notes:

    - The id *must* be a root folder like "users/foo" (not "users/foo/bar").
    - The authenticated user must own the asset root to see its quota usage.

  Args:
    rootId: The ID of the asset to check.

  Returns:
    A dict describing the asset's quota usage. Looks like, with size in bytes:
      {
          asset_count: {usage: number, limit: number},
          asset_size: {usage: number, limit: number},
      }
  """
  return _instance().getAssetRootQuota(rootId)


def getAssetAcl(assetId):
  """Returns the access control list of the asset with the given ID.

  Args:
    assetId: The ID of the asset to check.

  Returns:
    A dict describing the asset's ACL. Looks like:
      {
         "owners" : ["user@domain1.com"],
         "writers": ["user2@domain1.com", "user3@domain1.com"],
         "readers": ["some_group@domain2.com"],
         "all_users_can_read" : True
      }
  """
  return _instance().getAssetAcl(assetId)


def setAssetAcl(assetId, aclUpdate):
  """Sets the access control list of the asset with the given ID.

  The owner ACL cannot be changed, and the final ACL of the asset
  is constructed by merging the OWNER entries of the old ACL with
  the incoming ACL record.

  Args:
    assetId: The ID of the asset to set the ACL on.
    aclUpdate: The updated ACL for the asset. Must be formatted like the
        value returned by getAssetAcl but without "owners".
  """
  return _instance().setAssetAcl(assetId, aclUpdate)


def setAssetProperties(assetId, properties):
  """Sets metadata properties of the asset with the given ID.

  To delete a property, set its value to null.
  The authenticated user must be a writer or owner of the asset.

  Args:
    assetId: The ID of the asset to set the ACL on.
    properties: A dictionary of keys and values for the properties to update.
  """
  return _instance().setAssetProperties(assetId, properties)


def createAssetHome(requestedId):
  """Attempts to create a home root folder for the current user ("users/joe").

  Results in an error if the user already has a home root folder or the
  requested ID is unavailable.

  Args:
    requestedId: The requested ID of the home folder (e.g. "users/joe").
  """
  return _instance().createAssetHome(requestedId)


def authorizeHttp(http):
  return _instance().authorizeHttp(http)


def send_(path, params, opt_method='POST', opt_raw=False):
  """Send an API call.

  Args:
    path: The API endpoint to call.
    params: The call parameters.
    opt_method: The HTTPRequest method (GET or POST).
    opt_raw: Whether the data should be returned raw, without attempting
        to decode it as JSON.

  Returns:
    The data object returned by the API call.

  Raises:
    EEException: For malformed requests or errors from the server.
  """
  return _instance().send_(path, params, opt_method, opt_raw)


def create_assets(asset_ids, asset_type, mk_parents):
  """Creates the specified assets if they do not exist."""
  return _instance().create_assets(asset_ids, asset_type, mk_parents)


def setThreadCredentials(credentials):
  """Sets the credentials for the current thread.
  
  Args:
    credentials: The credentials to set.
    
  Returns:
    False if the same credentials are already set, otherwise True
  """
  currentCredentials = _LOCAL.__dict__.get('credentials', None)
  if currentCredentials is credentials:
    return False
  else:
    _LOCAL.credentials = credentials
    return True


def _instance():
  threadCredentials = _LOCAL.__dict__.get('credentials', None)
  if not threadCredentials: # Thread not initialized - use global state
    global _global_data
    if not _global_data:
      _global_data = _Data()
    return _global_data

  if 'data' in _LOCAL.__dict__:
    data = _LOCAL.data
    if threadCredentials is not data._credentials:
      data = None  # Don't reuse data object for different credentials
  else:
    data = _Data()
    _LOCAL.data = data
  return data


class _Data:
  def __init__(self):
    pass

  # OAuth2 credentials object.  This may be set by ee.Initialize().
  _credentials = None

  # The base URL for all data calls.  This is set by ee.initialize().
  _api_base_url = None

  # The base URL for map tiles.  This is set by ee.initialize().
  _tile_base_url = None

  # Whether the module has been initialized.
  _initialized = False

  # Sets the number of milliseconds to wait for a request before considering
  # it timed out. 0 means no limit.
  _deadline_ms = 0

  # A function called when profile results are received from the server. Takes the
  # profile ID as an argument. None if profiling is disabled.
  #
  # This is a global variable because the alternative is to add a parameter to
  # ee.data.send_, which would then have to be propagated from the assorted API
  # call functions (ee.data.getInfo, ee.data.getMapId, etc.), and the user would
  # have to modify each call to profile, rather than enabling profiling as a
  # wrapper around the entire program (with ee.data.profiling, defined below).
  _profile_hook = None

  def initialize(self, credentials=None, api_base_url=None, tile_base_url=None):
    """Initializes the data module, setting credentials and base URLs.
  
    If any of the arguments are unspecified, they will keep their old values;
    the defaults if initialize() has never been called before.
  
    Args:
      credentials: The OAuth2 credentials.
      api_base_url: The EarthEngine REST API endpoint.
      tile_base_url: The EarthEngine REST tile endpoint.
    """
    # If already initialized, only replace the explicitly specified parts.
    if credentials is not None:
      self._credentials = credentials

    if api_base_url is not None:
      self._api_base_url = api_base_url
    elif not self._initialized:
      self._api_base_url = DEFAULT_API_BASE_URL

    if tile_base_url is not None:
      self._tile_base_url = tile_base_url
    elif not self._initialized:
      self._tile_base_url = DEFAULT_TILE_BASE_URL

    self._initialized = True

  def reset(self):
    """Resets the data module, clearing credentials and custom base URLs."""
    self._credentials = None
    self._api_base_url = None
    self._tile_base_url = None
    self._initialized = False

  def setDeadline(self, milliseconds):
    """Sets the timeout length for API requests.
  
    Args:
      milliseconds: The number of milliseconds to wait for a request
          before considering it timed out. 0 means no limit.
    """
    self._deadline_ms = milliseconds

  @contextlib.contextmanager
  def profiling(self, hook):
    # pylint: disable=g-doc-return-or-yield
    """Returns a context manager which enables or disables profiling.
  
    If hook is not None, enables profiling for all API calls in its scope and
    calls the hook function with all resulting profile IDs. If hook is null,
    disables profiling (or leaves it disabled).
  
    Args:
      hook: A function of one argument which is called with each profile
          ID obtained from API calls, just before the API call returns.
    """
    saved_hook = self._profile_hook
    self._profile_hook = hook
    try:
      yield
    finally:
      self._profile_hook = saved_hook

  def getInfo(self, asset_id):
    """Load info for an asset, given an asset id.
  
    Args:
      asset_id: The asset to be retrieved.
  
    Returns:
      The value call results.
    """
    return self.send_('/info', {'id': asset_id})

  def getList(self, params):
    """Get a list of contents for a collection asset.
  
    Args:
      params: An object containing request parameters with the
          following possible values:
              id (string) The asset id of the collection to list.
              starttime (number) Start time, in msec since the epoch.
              endtime (number) End time, in msec since the epoch.
              fields (comma-separated strings) Field names to return.
  
    Returns:
      The list call results.
    """
    return self.send_('/list', params)

  def getMapId(self, params):
    """Get a Map ID for a given asset.
  
    Args:
      params: An object containing visualization options with the
              following possible values:
        image - (JSON string) The image to render.
        version - (number) Version number of image (or latest).
        bands - (comma-seprated strings) Comma-delimited list of
            band names to be mapped to RGB.
        min - (comma-separated numbers) Value (or one per band)
            to map onto 00.
        max - (comma-separated numbers) Value (or one per band)
            to map onto FF.
        gain - (comma-separated numbers) Gain (or one per band)
            to map onto 00-FF.
        bias - (comma-separated numbers) Offset (or one per band)
            to map onto 00-FF.
        gamma - (comma-separated numbers) Gamma correction
            factor (or one per band)
        palette - (comma-separated strings) A string of comma-separated
            CSS-style color strings (single-band previews only). For example,
            'FF0000,000000'.
        format (string) Either 'jpg' (does not support transparency) or
            'png' (supports transparency).
  
    Returns:
      A dictionary containing "mapid" and "token" strings, which can
      be combined to retrieve tiles from the /map service.
    """
    params['json_format'] = 'v2'
    return self.send_('/mapid', params)

  def getTileUrl(self, mapid, x, y, z):
    """Generate a URL for map tiles from a Map ID and coordinates.
  
    Args:
      mapid: The Map ID to generate tiles for, a dictionary containing "mapid"
          and "token" strings.
      x: The tile x coordinate.
      y: The tile y coordinate.
      z: The tile zoom level.
  
    Returns:
      The tile URL.
    """
    width = 2 ** z
    x %= width
    if x < 0:
      x += width
    return '%s/map/%s/%d/%d/%d?token=%s' % (
      self._tile_base_url, mapid['mapid'], z, x, y, mapid['token'])

  def getValue(self, params):
    """Retrieve a processed value from the front end.
  
    Args:
      params: A dictionary containing:
          json - (String) A JSON object to be evaluated.
  
    Returns:
      The value call results.
    """
    params['json_format'] = 'v2'
    return self.send_('/value', params)

  def getThumbnail(self, params):
    """Get a Thumbnail for a given asset.
  
    Args:
      params: Parameters identical to getMapId, plus:
          size - (a number or pair of numbers in format WIDTHxHEIGHT) Maximum
            dimensions of the thumbnail to render, in pixels. If only one number
            is passed, it is used as the maximum, and the other dimension is
            computed by proportional scaling.
          region - (E,S,W,N or GeoJSON) Geospatial region of the image
            to render. By default, the whole image.
          format - (string) Either 'png' (default) or 'jpg'.
  
    Returns:
      A thumbnail image as raw PNG data.
    """
    return self.send_('/thumb', params, opt_method='GET', opt_raw=True)

  def getThumbId(self, params):
    """Get a Thumbnail ID for a given asset.
  
    Args:
      params: Parameters identical to getMapId, plus:
          size - (a number or pair of numbers in format WIDTHxHEIGHT) Maximum
            dimensions of the thumbnail to render, in pixels. If only one number
            is passed, it is used as the maximum, and the other dimension is
            computed by proportional scaling.
          region - (E,S,W,N or GeoJSON) Geospatial region of the image
            to render. By default, the whole image.
          format - (string) Either 'png' (default) or 'jpg'.
  
    Returns:
      A thumbnail ID.
    """
    request = params.copy()
    request['getid'] = '1'
    request['json_format'] = 'v2'
    if 'size' in request and isinstance(request['size'], (list, tuple)):
      request['size'] = 'x'.join(map(str, request['size']))
    return self.send_('/thumb', request)

  def makeThumbUrl(self, thumbId):
    """Create a thumbnail URL from the given thumbid and token.
  
    Args:
      thumbId: An object containing a thumbnail thumbid and token.
  
    Returns:
      A URL from which the thumbnail can be obtained.
    """
    return '%s/api/thumb?thumbid=%s&token=%s' % (
      self._tile_base_url, thumbId['thumbid'], thumbId['token'])

  def getDownloadId(self, params):
    """Get a Download ID.
  
    Args:
      params: An object containing visualization options with the following
        possible values:
          name - a base name to use when constructing filenames.
          bands - a description of the bands to download. Must be an array of
              dictionaries, each with the following keys:
            id - the name of the band, a string, required.
            crs - an optional CRS string defining the band projection.
            crs_transform - an optional array of 6 numbers specifying an affine
                transform from the specified CRS, in the order: xScale,
                yShearing, xShearing, yScale, xTranslation and yTranslation.
            dimensions - an optional array of two integers defining the width and
                height to which the band is cropped.
            scale - an optional number, specifying the scale in meters of the
                   band; ignored if crs and crs_transform is specified.
          crs - a default CRS string to use for any bands that do not explicitly
              specify one.
          crs_transform - a default affine transform to use for any bands that do
              not specify one, of the same format as the crs_transform of bands.
          dimensions - default image cropping dimensions to use for any bands
              that do not specify them.
          scale - a default scale to use for any bands that do not specify one;
              ignored if crs and crs_transform is specified.
          region - a polygon specifying a region to download; ignored if crs
              and crs_transform is specified.
  
    Returns:
      A dict containing a docid and token.
    """
    params['json_format'] = 'v2'
    if 'bands' in params and not isinstance(params['bands'], six.string_types):
      params['bands'] = json.dumps(params['bands'])
    return self.send_('/download', params)

  def makeDownloadUrl(self, downloadId):
    """Create a download URL from the given docid and token.
  
    Args:
      downloadId: An object containing a download docid and token.
  
    Returns:
      A URL from which the download can be obtained.
    """
    return '%s/api/download?docid=%s&token=%s' % (
      self._tile_base_url, downloadId['docid'], downloadId['token'])

  def getTableDownloadId(self, params):
    """Get a Download ID.
  
    Args:
      params: An object containing table download options with the following
        possible values:
          format - The download format, CSV or JSON.
          selectors - Comma separated string of selectors that can be used to
              determine which attributes will be downloaded.
          filename - The name of the file that will be downloaded.
  
    Returns:
      A dict containing a docid and token.
    """
    params['json_format'] = 'v2'
    return self.send_('/table', params)

  def makeTableDownloadUrl(self, downloadId):
    """Create a table download URL from a docid and token.
  
    Args:
      downloadId: A table download id and token.
  
    Returns:
      A Url from which the download can be obtained.
    """
    return '%s/api/table?docid=%s&token=%s' % (
      self._tile_base_url, downloadId['docid'], downloadId['token'])

  def getAlgorithms(self):
    """Get the list of algorithms.
  
    Returns:
      The dictionary of algorithms.  Each algorithm is a dictionary containing
      the following fields:
          "description" - (string) A text description of the algorithm.
          "returns" - (string) The return type of the algorithm.
          "args" - An array of arguments.  Each argument specifies the following:
              "name" - (string) The name of the argument.
              "description" - (string) A text description of the argument.
              "type" - (string) The type of the argument.
              "optional" - (boolean) Whether the argument is optional or not.
              "default" - A representation of the default value if the argument
                  is not specified.
    """
    return self.send_('/algorithms', {}, 'GET')

  def createAsset(self, value, opt_path=None):
    """Creates an asset from a JSON value.
  
    To create an empty image collection or folder, pass in a "value" object
    with a "type" key whose value is "ImageCollection" or "Folder".
  
    Args:
      value: An object describing the asset to create or a JSON string
          with the already-serialized value for the new asset.
      opt_path: An optional desired ID, including full path.
  
    Returns:
      A description of the saved asset, including a generated ID.
    """
    if not isinstance(value, six.string_types):
      value = json.dumps(value)
    args = {'value': value, 'json_format': 'v2'}
    if opt_path is not None:
      args['id'] = opt_path
    return self.send_('/create', args)

  def copyAsset(self, sourceId, destinationId):
    """Copies the asset from sourceId into destinationId.
  
    Args:
      sourceId: The ID of the asset to copy.
      destinationId: The ID of the new asset created by copying.
    """
    self.send_('/copy', {
      'sourceId': sourceId,
      'destinationId': destinationId,
    })

  def renameAsset(self, sourceId, destinationId):
    """Renames the asset from sourceId to destinationId.
  
    Args:
      sourceId: The ID of the asset to rename.
      destinationId: The new ID of the asset.
    """
    self.send_('/rename', {
      'sourceId': sourceId,
      'destinationId': destinationId,
    })

  def deleteAsset(self, assetId):
    """Deletes the asset with the given id.
  
    Args:
      assetId: The ID of the asset to delete.
    """
    self.send_('/delete', {'id': assetId})

  def newTaskId(self, count=1):
    """Generate an ID for a long-running task.
  
    Args:
      count: Optional count of IDs to generate, one by default.
  
    Returns:
      A list containing generated ID strings.
    """
    args = {'count': count}
    return self.send_('/newtaskid', args)

  def getTaskList(self):
    """Retrieves a list of the user's tasks.
  
    Returns:
      A list of task status dictionaries, one for each task submitted to EE by
      the current user. These include currently running tasks as well as recently
      canceled or failed tasks.
    """
    return self.send_('/tasklist', {}, 'GET')['tasks']

  def getTaskStatus(self, taskId):
    """Retrieve status of one or more long-running tasks.
  
    Args:
      taskId: ID of the task or a list of multiple IDs.
  
    Returns:
      List containing one object for each queried task, in the same order as
      the input array, each object containing the following values:
        id (string) ID of the task.
        state (string) State of the task, one of READY, RUNNING, COMPLETED,
          FAILED, CANCELLED; or UNKNOWN if the task with the specified ID
          doesn't exist.
        error_message (string) For a FAILED task, a description of the error.
    """
    if isinstance(taskId, six.string_types):
      taskId = [taskId]
    args = {'q': ','.join(taskId)}
    return self.send_('/taskstatus', args, 'GET')

  def cancelTask(self, taskId):
    """Cancels a batch task."""
    self.send_('/updatetask', {'id': taskId, 'action': 'CANCEL'})

  def startProcessing(self, taskId, params):
    """Create processing task that exports or pre-renders an image.
  
    Args:
      taskId: ID for the task (obtained using newTaskId).
      params: The object that describes the processing task; only fields
        that are common for all processing types are documented below.
          type (string) Either 'EXPORT_IMAGE', 'EXPORT_FEATURES',
            'EXPORT_VIDEO', or 'EXPORT_TILES'.
          json (string) JSON description of the image.
  
    Returns:
      A dict with optional notes about the created task.
    """
    args = params.copy()
    args['id'] = taskId
    return self.send_('/processingrequest', args)

  def startIngestion(self, taskId, params):
    """Creates an asset import task.
  
    Args:
      taskId: ID for the task (obtained using newTaskId).
      params: The object that describes the import task, which can
          have these fields:
            id (string) The destination asset id (e.g. users/foo/bar).
            tilesets (array) A list of Google Cloud Storage source file paths
              formatted like:
                [{'sources': [
                    {'primaryPath': 'foo.tif', 'additionalPaths': ['foo.prj']},
                    {'primaryPath': 'bar.tif', 'additionalPaths': ['bar.prj'},
                ]}]
              Where path values correspond to source files' Google Cloud Storage
              object names, e.g. 'gs://bucketname/filename.tif'
            bands (array) An optional list of band names formatted like:
              [{'id': 'R'}, {'id': 'G'}, {'id': 'B'}]
  
    Returns:
      A dict with optional notes about the created task.
    """
    args = {'id': taskId, 'request': json.dumps(params)}
    return self.send_('/ingestionrequest', args)

  def getAssetRoots(self):
    """Returns the list of the root folders the user owns.
  
    Note: The "id" values for roots are two levels deep, e.g. "users/johndoe"
          not "users/johndoe/notaroot".
  
    Returns:
      A list of folder descriptions formatted like:
        [
            {"type": "Folder", "id": "users/foo"},
            {"type": "Folder", "id": "projects/bar"},
        ]
    """
    return self.send_('/buckets', {}, 'GET')

  def getAssetRootQuota(self, rootId):
    """Returns quota usage details for the asset root with the given ID.
  
    Usage notes:
  
      - The id *must* be a root folder like "users/foo" (not "users/foo/bar").
      - The authenticated user must own the asset root to see its quota usage.
  
    Args:
      rootId: The ID of the asset to check.
  
    Returns:
      A dict describing the asset's quota usage. Looks like, with size in bytes:
        {
            asset_count: {usage: number, limit: number},
            asset_size: {usage: number, limit: number},
        }
    """
    return self.send_('/quota', {'id': rootId}, 'GET')

  def getAssetAcl(self, assetId):
    """Returns the access control list of the asset with the given ID.
  
    Args:
      assetId: The ID of the asset to check.
  
    Returns:
      A dict describing the asset's ACL. Looks like:
        {
           "owners" : ["user@domain1.com"],
           "writers": ["user2@domain1.com", "user3@domain1.com"],
           "readers": ["some_group@domain2.com"],
           "all_users_can_read" : True
        }
    """
    return self.send_('/getacl', {'id': assetId}, 'GET')

  def setAssetAcl(self, assetId, aclUpdate):
    """Sets the access control list of the asset with the given ID.
  
    The owner ACL cannot be changed, and the final ACL of the asset
    is constructed by merging the OWNER entries of the old ACL with
    the incoming ACL record.
  
    Args:
      assetId: The ID of the asset to set the ACL on.
      aclUpdate: The updated ACL for the asset. Must be formatted like the
          value returned by getAssetAcl but without "owners".
    """
    self.send_('/setacl', {'id': assetId, 'value': aclUpdate})

  def setAssetProperties(self, assetId, properties):
    """Sets metadata properties of the asset with the given ID.
  
    To delete a property, set its value to null.
    The authenticated user must be a writer or owner of the asset.
  
    Args:
      assetId: The ID of the asset to set the ACL on.
      properties: A dictionary of keys and values for the properties to update.
    """
    self.send_('/setproperties', {'id': assetId, 'properties': json.dumps(properties)})

  def createAssetHome(self, requestedId):
    """Attempts to create a home root folder for the current user ("users/joe").
  
    Results in an error if the user already has a home root folder or the
    requested ID is unavailable.
  
    Args:
      requestedId: The requested ID of the home folder (e.g. "users/joe").
    """
    self.send_('/createbucket', {'id': requestedId})

  def authorizeHttp(self, http):
    if self._credentials:
      return self._credentials.authorize(http)
    else:
      return http

  def send_(self, path, params, opt_method='POST', opt_raw=False):
    """Send an API call.
  
    Args:
      path: The API endpoint to call.
      params: The call parameters.
      opt_method: The HTTPRequest method (GET or POST).
      opt_raw: Whether the data should be returned raw, without attempting
          to decode it as JSON.
  
    Returns:
      The data object returned by the API call.
  
    Raises:
      EEException: For malformed requests or errors from the server.
    """
    # Make sure we never perform API calls before initialization.
    self.initialize()

    if self._profile_hook:
      params = params.copy()
      params['profiling'] = '1'

    url = self._api_base_url + path
    headers = {}

    try:
      payload = urllib.parse.urlencode(params)  # Python 3.x
    except AttributeError:
      payload = urllib.urlencode(params)  # Python 2.x
    http = httplib2.Http(timeout=(self._deadline_ms / 1000.0) or None)
    http = self.authorizeHttp(http)

    if opt_method == 'GET':
      url = url + ('&' if '?' in url else '?') + payload
      payload = None
    elif opt_method == 'POST':
      headers['Content-type'] = 'application/x-www-form-urlencoded'
    else:
      raise ee_exception.EEException('Unexpected request method: ' + opt_method)

    def send_with_backoff(retries=0):
      """Send an API call with backoff.
  
      Attempts an API call. If the server's response has a 429 status, retry the
      request using an incremental backoff strategy.
  
      Args:
        retries: The number of retries that have already occurred.
  
      Returns:
        A tuple of response, content returned by the API call.
  
      Raises:
        EEException: For errors from the server.
      """
      try:
        response, content = http.request(url, method=opt_method, body=payload,
                                         headers=headers)
        if response.status == 429:
          if retries < MAX_RETRIES:
            time.sleep(min(2 ** retries * BASE_RETRY_WAIT, MAX_RETRY_WAIT) / 1000)
            response, content = send_with_backoff(retries + 1)
      except httplib2.HttpLib2Error as e:
        raise ee_exception.EEException(
          'Unexpected HTTP error: %s' % e.message)
      return response, content

    response, content = send_with_backoff()

    # Call the profile hook if present. Note that this is done before we handle
    # the content, so that profiles are reported even if the response is an error.
    if self._profile_hook and _PROFILE_HEADER_LOWERCASE in response:
      self._profile_hook(response[_PROFILE_HEADER_LOWERCASE])

    # Whether or not the response is an error, it may be JSON.
    content_type = (response['content-type'] or 'application/json').split(';')[0]
    if content_type in ('application/json', 'text/json') and not opt_raw:
      try:
        try:
          # Python 3.x
          try:
            content = content.decode()
          except AttributeError:
            pass
        except UnicodeDecodeError:
          # Python 2.x
          content = content
        json_content = json.loads(content)
      except Exception:
        raise ee_exception.EEException('Invalid JSON: %s' % content)
      if 'error' in json_content:
        raise ee_exception.EEException(json_content['error']['message'])
      if 'data' not in content:
        raise ee_exception.EEException('Malformed response: ' + str(content))
    else:
      json_content = None

    if response.status < 100 or response.status >= 300:
      # Note if the response is JSON and contains an error value, we raise that
      # error above rather than this generic one.
      raise ee_exception.EEException('Server returned HTTP code: %d' %
                                     response.status)

    # Now known not to be an error response...
    if opt_raw:
      return content
    elif json_content is None:
      raise ee_exception.EEException(
        'Response was unexpectedly not JSON, but %s' % response['content-type'])
    else:
      return json_content['data']

  def create_assets(self, asset_ids, asset_type, mk_parents):
    """Creates the specified assets if they do not exist."""
    for asset_id in asset_ids:
      if self.getInfo(asset_id):
        print('Asset %s already exists' % asset_id)
        continue
      if mk_parents:
        parts = asset_id.split('/')
        path = ''
        for part in parts[:-1]:
          path += part
          if self.getInfo(path) is None:
            self.createAsset({'type': ASSET_TYPE_FOLDER}, path)
          path += '/'
      self.createAsset({'type': asset_type}, asset_id)
