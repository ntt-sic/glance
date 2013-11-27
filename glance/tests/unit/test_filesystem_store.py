# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2011 OpenStack Foundation
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

"""Tests the filesystem backend store"""

import __builtin__
import errno
import hashlib
import json
import os
import StringIO

import fixtures
from oslo.config import cfg
import mox
import stubout

from glance.common import exception
from glance.openstack.common import uuidutils
from glance.openstack.common import processutils
from glance.store.filesystem import Store, ChunkedFile
from glance.store.location import get_location_from_uri
from glance.tests.unit import base


CONF = cfg.CONF
CONF.import_opt('filesystem_store_datadir', 'glance.store.filesystem')

class TestStore(base.IsolatedUnitTest):

    def setUp(self):
        """Establish a clean test environment"""
        super(TestStore, self).setUp()
        self.orig_chunksize = ChunkedFile.CHUNKSIZE
        ChunkedFile.CHUNKSIZE = 10
        self.store = Store()
        self.stubs = stubout.StubOutForTesting()
        #self.stubs.Set(processutils, 'execute', self._fake_execute)
        #self.stubs.Set(self.store, '_find_best_datadir',
        #               self.fake_find_best_datadir)

    def tearDown(self):
        """Clear the test environment"""
        CONF.filesystem_store_datadir = self.test_dir
        super(TestStore, self).tearDown()
        ChunkedFile.CHUNKSIZE = self.orig_chunksize

    def _fake_execute(self, *cmd, **kwargs):
        """Writen for _get_capacity_info will always return total_size."""
        if cmd[0] == 'stat':
            return ['8096 1020079']
        else:
            return ["0\t/test/fake/dir"]

    def fake_find_best_datadir(self, imagesize):
        """Fakes best datadir to return expected datadirectory."""
        return self.test_dir

    def fake_get_filesystem_store_datadir_conf(self):
        return [self.test_dir]

    def test_get(self):
        """Test a "normal" retrieval of an image in chunks"""
        # First add an image...
        self.stubs.Set(self.store, '_find_best_datadir',
                       self.fake_find_best_datadir)
        image_id = uuidutils.generate_uuid()
        file_contents = "chunk00000remainder"
        image_file = StringIO.StringIO(file_contents)

        location, size, checksum, _ = self.store.add(image_id,
                                                     image_file,
                                                     len(file_contents))

        # Now read it back...
        uri = "file:///%s/%s" % (self.test_dir, image_id)
        loc = get_location_from_uri(uri)
        (image_file, image_size) = self.store.get(loc)

        expected_data = "chunk00000remainder"
        expected_num_chunks = 2
        data = ""
        num_chunks = 0

        for chunk in image_file:
            num_chunks += 1
            data += chunk
        self.assertEqual(expected_data, data)
        self.assertEqual(expected_num_chunks, num_chunks)

    def test_get_non_existing(self):
        """
        Test that trying to retrieve a file that doesn't exist
        raises an error
        """
        loc = get_location_from_uri("file:///%s/non-existing" % self.test_dir)
        self.assertRaises(exception.NotFound,
                          self.store.get,
                          loc)

    def test_add(self):
        """Test that we can add an image via the filesystem backend"""
        self.stubs.Set(self.store, '_find_best_datadir',
                       self.fake_find_best_datadir)
        ChunkedFile.CHUNKSIZE = 1024
        expected_image_id = uuidutils.generate_uuid()
        expected_file_size = 1024 * 5  # 5K
        expected_file_contents = "*" * expected_file_size
        expected_checksum = hashlib.md5(expected_file_contents).hexdigest()
        expected_location = "file://%s/%s" % (self.test_dir,
                                              expected_image_id)
        image_file = StringIO.StringIO(expected_file_contents)

        location, size, checksum, _ = self.store.add(expected_image_id,
                                                     image_file,
                                                     expected_file_size)

        self.assertEqual(expected_location, location)
        self.assertEqual(expected_file_size, size)
        self.assertEqual(expected_checksum, checksum)

        uri = "file:///%s/%s" % (self.test_dir, expected_image_id)
        loc = get_location_from_uri(uri)
        (new_image_file, new_image_size) = self.store.get(loc)
        new_image_contents = ""
        new_image_file_size = 0

        for chunk in new_image_file:
            new_image_file_size += len(chunk)
            new_image_contents += chunk

        self.assertEqual(expected_file_contents, new_image_contents)
        self.assertEqual(expected_file_size, new_image_file_size)

    def test_add_check_metadata_success(self):
        self.stubs.Set(self.store, '_find_best_datadir',
                       self.fake_find_best_datadir)
        expected_image_id = uuidutils.generate_uuid()
        in_metadata = {'akey': u'some value', 'list': [u'1', u'2', u'3']}
        jsonfilename = os.path.join(self.test_dir,
                                    "storage_metadata.%s" % expected_image_id)

        self.config(filesystem_store_metadata_file=jsonfilename)
        with open(jsonfilename, 'w') as fptr:
            json.dump(in_metadata, fptr)
        expected_file_size = 10
        expected_file_contents = "*" * expected_file_size
        image_file = StringIO.StringIO(expected_file_contents)

        location, size, checksum, metadata = self.store.add(expected_image_id,
                                                            image_file,
                                                            expected_file_size)

        self.assertEqual(metadata, in_metadata)

    def test_add_check_metadata_bad_data(self):
        self.stubs.Set(self.store, '_find_best_datadir',
                       self.fake_find_best_datadir)
        expected_image_id = uuidutils.generate_uuid()
        in_metadata = {'akey': 10}  # only unicode is allowed
        jsonfilename = os.path.join(self.test_dir,
                                    "storage_metadata.%s" % expected_image_id)

        self.config(filesystem_store_metadata_file=jsonfilename)
        with open(jsonfilename, 'w') as fptr:
            json.dump(in_metadata, fptr)
        expected_file_size = 10
        expected_file_contents = "*" * expected_file_size
        image_file = StringIO.StringIO(expected_file_contents)

        location, size, checksum, metadata = self.store.add(expected_image_id,
                                                            image_file,
                                                            expected_file_size)

        self.assertEqual(metadata, {})

    def test_add_check_metadata_bad_nosuch_file(self):
        self.stubs.Set(self.store, '_find_best_datadir',
                       self.fake_find_best_datadir)
        expected_image_id = uuidutils.generate_uuid()
        jsonfilename = os.path.join(self.test_dir,
                                    "storage_metadata.%s" % expected_image_id)

        self.config(filesystem_store_metadata_file=jsonfilename)
        expected_file_size = 10
        expected_file_contents = "*" * expected_file_size
        image_file = StringIO.StringIO(expected_file_contents)

        location, size, checksum, metadata = self.store.add(expected_image_id,
                                                            image_file,
                                                            expected_file_size)

        self.assertEqual(metadata, {})

    def test_add_already_existing(self):
        """
        Tests that adding an image with an existing identifier
        raises an appropriate exception
        """
        self.stubs.Set(self.store, '_find_best_datadir',
                       self.fake_find_best_datadir)
        ChunkedFile.CHUNKSIZE = 1024
        image_id = uuidutils.generate_uuid()
        file_size = 1024 * 5  # 5K
        file_contents = "*" * file_size
        image_file = StringIO.StringIO(file_contents)

        location, size, checksum, _ = self.store.add(image_id,
                                                     image_file,
                                                     file_size)
        image_file = StringIO.StringIO("nevergonnamakeit")
        self.assertRaises(exception.Duplicate,
                          self.store.add,
                          image_id, image_file, 0)

    def _do_test_add_write_failure(self, errno, exception):
        self.stubs.Set(self.store, '_find_best_datadir',
                       self.fake_find_best_datadir)
        ChunkedFile.CHUNKSIZE = 1024
        image_id = uuidutils.generate_uuid()
        file_size = 1024 * 5  # 5K
        file_contents = "*" * file_size
        path = os.path.join(self.test_dir, image_id)
        image_file = StringIO.StringIO(file_contents)

        m = mox.Mox()
        m.StubOutWithMock(__builtin__, 'open')
        e = IOError()
        e.errno = errno
        open(path, 'wb').AndRaise(e)
        m.ReplayAll()

        try:
            self.assertRaises(exception,
                              self.store.add,
                              image_id, image_file, 0)
            self.assertFalse(os.path.exists(path))
        finally:
            m.VerifyAll()
            m.UnsetStubs()

    def test_add_storage_full(self):
        """
        Tests that adding an image without enough space on disk
        raises an appropriate exception
        """
        self._do_test_add_write_failure(errno.ENOSPC, exception.StorageFull)

    def test_add_file_too_big(self):
        """
        Tests that adding an excessively large image file
        raises an appropriate exception
        """
        self._do_test_add_write_failure(errno.EFBIG, exception.StorageFull)

    def test_add_storage_write_denied(self):
        """
        Tests that adding an image with insufficient filestore permissions
        raises an appropriate exception
        """
        self._do_test_add_write_failure(errno.EACCES,
                                        exception.StorageWriteDenied)

    def test_add_other_failure(self):
        """
        Tests that a non-space-related IOError does not raise a
        StorageFull exception.
        """
        self._do_test_add_write_failure(errno.ENOTDIR, IOError)

    def test_add_cleanup_on_read_failure(self):
        """
        Tests the partial image file is cleaned up after a read
        failure.
        """
        self.stubs.Set(self.store, '_find_best_datadir',
                       self.fake_find_best_datadir)
        ChunkedFile.CHUNKSIZE = 1024
        image_id = uuidutils.generate_uuid()
        file_size = 1024 * 5  # 5K
        file_contents = "*" * file_size
        path = os.path.join(self.test_dir, image_id)
        image_file = StringIO.StringIO(file_contents)

        def fake_Error(size):
            raise AttributeError()

        self.stubs.Set(image_file, 'read', fake_Error)

        self.assertRaises(AttributeError,
                          self.store.add,
                          image_id, image_file, 0)
        self.assertFalse(os.path.exists(path))

    def test_add_can_not_find_best_datadir(self):
        """
        Tests if StorageFull exception is raised on unsuccessful attempt
        to find best datadir while adding an image.
        """
        #self.stubs.UnsetAll()
        image_id = uuidutils.generate_uuid()
        file_size = 1024 * 5  # 5K
        file_contents = "*" * file_size
        path = os.path.join(self.test_dir, image_id)
        image_file = StringIO.StringIO(file_contents)

        def fake_get_capacity_info(datadir):
            return 0

        self.stubs.Set(self.store,
                       '_get_capacity_info',
                       fake_get_capacity_info)

        self.assertRaises(exception.StorageFull,
                          self.store.add,
                          image_id, image_file, 100)

    def test_find_best_datadir_single_datadir(self):
        """
        Test if multiple_datadirs is false if only one datadir is specified
        and same directory is returned.
        """
        fake_image_size = 100
        CONF.filesystem_store_datadir = [self.test_dir]
        self.store.configure_add()
        self.assertEqual(self.test_dir,
                         self.store._find_best_datadir(fake_image_size))

    def test_find_best_datadir_StorageFull(self):
        """
        Test StorageFull exception is raised if all specified datadirs
        cannot store the image.
        """
        fake_image_size = 100
        priority_2_datadir1 = '%s:2' % self.useFixture(fixtures.TempDir()).path
        priority_2_datadir2 = '%s:2' % self.useFixture(fixtures.TempDir()).path
        priority_1_datadir = '%s:1' % self.useFixture(fixtures.TempDir()).path
        priority_0_datadir = self.useFixture(fixtures.TempDir()).path
        CONF.filesystem_store_datadir = [priority_2_datadir1,
                                         priority_2_datadir2,
                                         priority_1_datadir,
                                         priority_0_datadir]
        self.store.configure_add()

        def fake_get_capacity_info(datadir):
            return 0

        self.stubs.Set(self.store, '_get_capacity_info',
                       fake_get_capacity_info)

        self.assertRaises(exception.StorageFull, self.store._find_best_datadir,
                          fake_image_size)

    def test_find_best_datadir(self):
        """Test if datadir is returned for a very small image."""
        fake_image_size = 1
        priority_2_datadir1 = '%s:2' % self.useFixture(fixtures.TempDir()).path
        priority_1_datadir = '%s:1' % self.useFixture(fixtures.TempDir()).path
        CONF.filesystem_store_datadir = [priority_2_datadir1,
                                         priority_1_datadir]
        self.stubs.Set(processutils, 'execute', self._fake_execute)
        self.store.configure_add()
        expected_data_dir = priority_2_datadir1.split(':')[0]
        self.assertEqual(expected_data_dir,
                         self.store._find_best_datadir(fake_image_size))

    def test_create_image_directories_OSError(self):
        """Test if OSError is caught attempt to create a datadir fails."""
        test_image_dirs = ['/tmp/test_dir1']

        def _fake_makedirs(datadir):
            raise OSError

        self.stubs.Set(os, 'makedirs', _fake_makedirs)
        self.assertRaises(exception.BadStoreConfiguration,
                          self.store._create_image_directories,
                          test_image_dirs)

    def test_create_image_directories(self):
        """Test if OSError is caught attempt to create a datadir fails."""
        test_image_dirs = ['/tmp/test_dir']
        self.store._create_image_directories(test_image_dirs)
        self.assertTrue(os.path.exists(test_image_dirs[0]))

    def test_delete(self):
        """
        Test we can delete an existing image in the filesystem store
        """
        # First add an image
        self.stubs.Set(self.store, '_find_best_datadir',
                       self.fake_find_best_datadir)
        image_id = uuidutils.generate_uuid()
        file_size = 1024 * 5  # 5K
        file_contents = "*" * file_size
        image_file = StringIO.StringIO(file_contents)

        location, size, checksum, _ = self.store.add(image_id,
                                                     image_file,
                                                     file_size)

        # Now check that we can delete it
        uri = "file:///%s/%s" % (self.test_dir, image_id)
        loc = get_location_from_uri(uri)
        self.store.delete(loc)

        self.assertRaises(exception.NotFound, self.store.get, loc)

    def test_delete_non_existing(self):
        """
        Test that trying to delete a file that doesn't exist
        raises an error
        """
        loc = get_location_from_uri("file:///tmp/glance-tests/non-existing")
        self.assertRaises(exception.NotFound,
                          self.store.delete,
                          loc)
