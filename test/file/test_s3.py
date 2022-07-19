import unittest

from file.s3 import S3


class TestS3(unittest.TestCase):
    def setUp(self) -> None:
        self.sut = S3()
        self.test_bucket = "unit-testing-buckets"

    def test_create_list_delete_buckets(self):
        # Arrange (and act)
        self.sut.create_bucket(bucket_name=self.test_bucket)

        # Act
        output = self.sut.list_buckets()

        # Assert
        self.assertGreater(len(output), 0)
        self.assertIn(self.test_bucket, output)

        # Teardown (and assert)
        self.sut.delete_bucket(self.test_bucket)
        reverted_buckets = self.sut.list_buckets()
        self.assertNotIn(self.test_bucket, reverted_buckets)
        self.assertEqual(len(output), len(reverted_buckets) + 1)

    def test_create_get_list_delete_object(self):
        # Arrange (and act)
        self.sut.create_bucket(bucket_name=self.test_bucket)
        mock_file_contents = b"\x00\x00\x00\x00\x00\x00\x00\x00\x01\x01\x01\x01\x01\x01"

        # Act
        self.sut.put_object(
            "rubbish_test_key",
            mock_file_contents,
            self.test_bucket,
        )

        output = self.sut.get_object_body(
            "rubbish_test_key",
            self.test_bucket
        )

        # Assert
        self.assertIn("rubbish_test_key", self.sut.list_objects(self.test_bucket))
        self.assertListEqual(
            ["rubbish_test_key"],
            self.sut.list_objects(
                self.test_bucket,
                prefix_filter="rubbish_test_key"
            )
        )
        self.assertEqual(output.read(), mock_file_contents)

        # Teardown (and Assert)
        self.sut.delete_objects(["rubbish_test_key"], self.test_bucket)
        self.assertNotIn("rubbish_test_key", self.sut.list_objects(self.test_bucket))

    def test_list_object_max_keys(self):
        # Arrange (and act)
        self.sut.create_bucket(bucket_name=self.test_bucket)
        mock_file_contents = b"\x00\x00\x00\x00\x00\x00\x00\x00\x01\x01\x01\x01\x01\x01"
        keys = [f"rubbish_max_keys_{n}" for n in range(5)]

        for key in keys:
            self.sut.put_object(
                key,
                mock_file_contents,
                self.test_bucket,
            )

        # Act and Assert
        self.assertEqual(len(self.sut.list_objects(self.test_bucket)), 5)
        self.assertEqual(len(self.sut.list_objects(self.test_bucket, max_keys=2)), 2)

    def test_list_object_with_truncated_keys(self):
        # Arrange (and act)
        self.sut.create_bucket(bucket_name=self.test_bucket)
        mock_file_contents = b"\x00\x00\x00\x00\x00\x00\x00\x00\x01\x01\x01\x01\x01\x01"
        keys = [f"rubbish_max_keys_{n}" for n in range(100)]

        for key in keys:
            self.sut.put_object(
                key,
                mock_file_contents,
                self.test_bucket,
            )

        # Act
        output_get_all = self.sut.list_objects(
            self.test_bucket,
            max_keys=10,  # force truncation
            get_all=True,
        )

        output_not_get_all = self.sut.list_objects(
            self.test_bucket,
            max_keys=10,  # force truncation
            get_all=False,
        )

        # Assert
        self.assertEqual(len(output_get_all), 100)
        self.assertEqual(len(output_not_get_all), 10)

    def tearDown(self) -> None:
        existing_buckets = self.sut.list_buckets()
        if self.test_bucket in existing_buckets:
            items = self.sut.list_objects(self.test_bucket, get_all=True)
            if items:
                self.sut.delete_objects(items, self.test_bucket)

            self.sut.delete_bucket(self.test_bucket)
