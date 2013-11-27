# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2010 OpenStack Foundation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
A simple filesystem-backed store
"""

import errno
import hashlib
import json
import os
import urlparse

from oslo.config import cfg

from glance.common import exception
from glance.common import utils
import glance.openstack.common.log as logging
from glance.openstack.common import processutils
import glance.store
import glance.store.base
import glance.store.location

LOG = logging.getLogger(__name__)

filesystem_opts = [
    cfg.ListOpt('filesystem_store_datadir', default=[],
                help=_('List of directories to which the Filesystem backend '
                       'store writes images.')),
    cfg.StrOpt('filesystem_store_metadata_file',
               help=_("The path to a file which contains the "
                      "metadata to be returned with any location "
                      "associated with this store.  The file must "
                      "contain a valid JSON dict."))]

CONF = cfg.CONF
CONF.register_opts(filesystem_opts)


class StoreLocation(glance.store.location.StoreLocation):

    """Class describing a Filesystem URI"""

    def process_specs(self):
        self.scheme = self.specs.get('scheme', 'file')
        self.path = self.specs.get('path')

    def get_uri(self):
        return "file://%s" % self.path

    def parse_uri(self, uri):
        """
        Parse URLs. This method fixes an issue where credentials specified
        in the URL are interpreted differently in Python 2.6.1+ than prior
        versions of Python.
        """
        pieces = urlparse.urlparse(uri)
        assert pieces.scheme in ('file', 'filesystem')
        self.scheme = pieces.scheme
        path = (pieces.netloc + pieces.path).strip()
        if path == '':
            reason = _("No path specified in URI: %s") % uri
            LOG.debug(reason)
            raise exception.BadStoreUri('No path specified')
        self.path = path


class ChunkedFile(object):

    """
    We send this back to the Glance API server as
    something that can iterate over a large file
    """

    CHUNKSIZE = 65536

    def __init__(self, filepath):
        self.filepath = filepath
        self.fp = open(self.filepath, 'rb')

    def __iter__(self):
        """Return an iterator over the image file"""
        try:
            if self.fp:
                while True:
                    chunk = self.fp.read(ChunkedFile.CHUNKSIZE)
                    if chunk:
                        yield chunk
                    else:
                        break
        finally:
            self.close()

    def close(self):
        """Close the internal file pointer"""
        if self.fp:
            self.fp.close()
            self.fp = None


class Store(glance.store.base.Store):

    def get_schemes(self):
        return ('file', 'filesystem')

    def _create_image_directories(self, directory_paths):
        """
        Create directories to write image files if
        it does not exist.

        :directory_paths is a list of directories belonging to glance store.
        :raise BadStoreConfiguration exception if creating a directory fails.
        """
        for datadir in directory_paths:
            if not os.path.exists(datadir):
                msg = _("Directory to write image files does not exist "
                        "(%s). Creating.") % datadir
                LOG.info(msg)
                try:
                    os.makedirs(datadir)
                except (IOError, OSError):
                    if os.path.exists(datadir):
                        # NOTE(markwash): If the path now exists, some other
                        # process must have beat us in the race condition.
                        # But it doesn't hurt, so we can safely ignore
                        # the error.
                        continue
                    reason = _("Unable to create datadir: %s") % datadir
                    LOG.error(reason)
                    raise exception.BadStoreConfiguration(
                        store_name="filesystem", reason=reason)

    def configure_add(self):
        """
        Configure the Store to use the stored configuration options
        Any store that needs special configuration should implement
        this method. If the store was not able to successfully configure
        itself, it should raise `exception.BadStoreConfiguration`

        BadStoreConfiguration is raised in following scenarios.
        1. filesystem_store_datadir param is not present in glance-api.conf
        2. priority specified along with image directories is not mumeric
        """
        if not CONF.filesystem_store_datadir:
            reason = (_("Could not find %s in configuration options.") %
                      'filesystem_store_datadir')
            LOG.error(reason)
            raise exception.BadStoreConfiguration(store_name="filesystem",
                                                  reason=reason)

        directory_paths = set()
        if len(CONF.filesystem_store_datadir) == 1:
            self.multiple_datadirs = False
            self.datadir = CONF.filesystem_store_datadir[0]
            directory_paths.add(self.datadir)
        else:
            self.multiple_datadirs = True
            self.priority_data_map = {}
            for datadir in CONF.filesystem_store_datadir:
                priority = 0
                parts = map(lambda x: x.strip(), datadir.split(":"))
                datadir_path = parts[0]
                if len(parts) == 2 and parts[1]:
                    priority = parts[1]
                    if not priority.isdigit():
                        msg = (_("Invalid priority value %s in %s "
                                 "configuration"))
                        LOG.warn(msg  % (priority, "filesystem"))
                        raise exception.BadStoreConfiguration(
                            store_name="filesystem", reason=reason)

                if datadir_path:
                    directory_paths.add(datadir_path)
                    self.priority_data_map.setdefault(int(priority),
                        []).append(datadir_path)

        self.priority_list = sorted(self.priority_data_map, reverse=True)
        self._create_image_directories(directory_paths)

    @staticmethod
    def _resolve_location(location):
        filepath = location.store_location.path

        if not os.path.exists(filepath):
            raise exception.NotFound(_("Image file %s not found") % filepath)

        filesize = os.path.getsize(filepath)
        return filepath, filesize

    def _get_metadata(self):
        if CONF.filesystem_store_metadata_file is None:
            return {}

        try:
            with open(CONF.filesystem_store_metadata_file, 'r') as fptr:
                metadata = json.load(fptr)
            glance.store.check_location_metadata(metadata)
            return metadata
        except glance.store.BackendException as bee:
            LOG.error(_('The JSON in the metadata file %s could not be used: '
                        '%s  An empty dictionary will be returned '
                        'to the client.')
                      % (CONF.filesystem_store_metadata_file, str(bee)))
            return {}
        except IOError as ioe:
            LOG.error(_('The path for the metadata file %s could not be '
                        'opened: %s  An empty dictionary will be returned '
                        'to the client.')
                      % (CONF.filesystem_store_metadata_file, ioe))
            return {}
        except Exception as ex:
            LOG.exception(_('An error occured processing the storage systems '
                            'meta data file: %s.  An empty dictionary will be '
                            'returned to the client.') % str(ex))
            return {}

    def get(self, location):
        """
        Takes a `glance.store.location.Location` object that indicates
        where to find the image file, and returns a tuple of generator
        (for reading the image file) and image_size

        :param location `glance.store.location.Location` object, supplied
                        from glance.store.location.get_location_from_uri()
        :raises `glance.exception.NotFound` if image does not exist
        """
        filepath, filesize = self._resolve_location(location)
        msg = _("Found image at %s. Returning in ChunkedFile.") % filepath
        LOG.debug(msg)
        return (ChunkedFile(filepath), filesize)

    def get_size(self, location):
        """
        Takes a `glance.store.location.Location` object that indicates
        where to find the image file and returns the image size

        :param location `glance.store.location.Location` object, supplied
                        from glance.store.location.get_location_from_uri()
        :raises `glance.exception.NotFound` if image does not exist
        :rtype int
        """
        filepath, filesize = self._resolve_location(location)
        msg = _("Found image at %s.") % filepath
        LOG.debug(msg)
        return filesize

    def delete(self, location):
        """
        Takes a `glance.store.location.Location` object that indicates
        where to find the image file to delete

        :location `glance.store.location.Location` object, supplied
                  from glance.store.location.get_location_from_uri()

        :raises NotFound if image does not exist
        :raises Forbidden if cannot delete because of permissions
        """
        loc = location.store_location
        fn = loc.path
        if os.path.exists(fn):
            try:
                LOG.debug(_("Deleting image at %(fn)s"), {'fn': fn})
                os.unlink(fn)
            except OSError:
                raise exception.Forbidden(_("You cannot delete file %s") % fn)
        else:
            raise exception.NotFound(_("Image file %s does not exist") % fn)

    def _get_capacity_info(self, mount_point):
        """Calculate free space on store."""

        #Calculate total space
        df = processutils.execute("stat", "-f", "-c", "'%S %b'",
                                  mount_point)[0].strip("'\n'")
        block_size, blocks_total = map(int, df.split())
        total_size = block_size * blocks_total

        #Calculate total allocated space
        du = processutils.execute("du", "-sb", "--apparent-size",
                                      mount_point)[0].strip("'\n'")
        total_allocated = int(du.split('\t')[0])

        return max(0, total_size - total_allocated)

    def _find_best_datadir(self, image_size):
        """Finds best datadir based on free space available.

        Traverses all glance datadirs based in order of their priority
        and return datadir that has maximum free space to accomodate an image.
        Stores with no priority are checked last.
        :image_size size of image being uploaded.
        :returns best_datadir as directory path of the best priority datadir.
        :raises exception.StorageFull if there is no datadir in
                self.priority_data_map that can accomodate the image.
        """
        best_datadir = None
        max_free_space = 0
        if not self.multiple_datadirs:
            return self.datadir

        for priority in self.priority_list:
            for datadir in self.priority_data_map.get(priority):
                free_space = self._get_capacity_info(datadir)
                if free_space >= image_size and free_space > max_free_space:
                    max_free_space = free_space
                    best_datadir = datadir

            # If best datadir is found with maximum free space for a priority
            # then break the loop, else continue to look up in the group
            # with lower priority datadirs.
            if best_datadir:
                break
        else:
            # Raise StorageFull if image can not be accomodated in
            # any available datadir.
            raise exception.StorageFull()

        return best_datadir

    def add(self, image_id, image_file, image_size):
        """
        Stores an image file with supplied identifier to the backend
        storage system and returns a tuple containing information
        about the stored image.

        :param image_id: The opaque image identifier
        :param image_file: The image data to write, as a file-like object
        :param image_size: The size of the image data to write, in bytes

        :retval tuple of URL in backing store, bytes written, checksum
                and a dictionary with storage system specific information
        :raises `glance.common.exception.Duplicate` if the image already
                existed

        :note By default, the backend writes the image data to a file
              `/<DATADIR>/<ID>`, where <DATADIR> is the value of
              the filesystem_store_datadir configuration option and <ID>
              is the supplied image ID.
        """
        datadir = self._find_best_datadir(image_size)
        filepath = os.path.join(datadir, str(image_id))

        if os.path.exists(filepath):
            raise exception.Duplicate(_("Image file %s already exists!")
                                      % filepath)

        checksum = hashlib.md5()
        bytes_written = 0
        try:
            with open(filepath, 'wb') as f:
                for buf in utils.chunkreadable(image_file,
                                               ChunkedFile.CHUNKSIZE):
                    bytes_written += len(buf)
                    checksum.update(buf)
                    f.write(buf)
        except IOError as e:
            if e.errno != errno.EACCES:
                self._delete_partial(filepath, image_id)
            exceptions = {errno.EFBIG: exception.StorageFull(),
                          errno.ENOSPC: exception.StorageFull(),
                          errno.EACCES: exception.StorageWriteDenied()}
            raise exceptions.get(e.errno, e)
        except:
            self._delete_partial(filepath, image_id)
            raise

        checksum_hex = checksum.hexdigest()
        metadata = self._get_metadata()

        LOG.debug(_("Wrote %(bytes_written)d bytes to %(filepath)s with "
                    "checksum %(checksum_hex)s"),
                  {'bytes_written': bytes_written,
                   'filepath': filepath,
                   'checksum_hex': checksum_hex})
        return ('file://%s' % filepath, bytes_written, checksum_hex, metadata)

    @staticmethod
    def _delete_partial(filepath, id):
        try:
            os.unlink(filepath)
        except Exception as e:
            msg = _('Unable to remove partial image data for image %s: %s')
            LOG.error(msg % (id, e))
