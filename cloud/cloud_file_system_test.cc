// Copyright (c) 2017 Rockset

#include "rocksdb/cloud/cloud_file_system.h"

#include "cloud/cloud_log_controller_impl.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/cloud/cloud_storage_provider_impl.h"
#include "rocksdb/convenience.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

TEST(CloudFileSystemTest, TestBucket) {
  CloudFileSystemOptions copts;
  copts.src_bucket.SetRegion("North");
  copts.src_bucket.SetBucketName("Input", "src.");
  ASSERT_FALSE(copts.src_bucket.IsValid());
  copts.src_bucket.SetObjectPath("Here");
  ASSERT_TRUE(copts.src_bucket.IsValid());

  copts.dest_bucket.SetRegion("South");
  copts.dest_bucket.SetObjectPath("There");
  ASSERT_FALSE(copts.dest_bucket.IsValid());
  copts.dest_bucket.SetBucketName("Output", "dest.");
  ASSERT_TRUE(copts.dest_bucket.IsValid());
}

TEST(CloudFileSystemTest, ConfigureOptions) {
  ConfigOptions config_options;
  CloudFileSystemOptions copts, copy;
  copts.local_sst_file_mode = LocalSstFileMode::kRemotePrimary;
  copts.keep_local_log_files = false;
  copts.create_bucket_if_missing = false;
  copts.validate_filesize = false;
  copts.skip_dbid_verification = false;
  copts.resync_on_open = false;
  copts.skip_cloud_files_in_getchildren = false;
  copts.constant_sst_file_size_in_sst_file_manager = 100;
  copts.run_purger = false;
  copts.purger_periodicity_millis = 101;

  std::string str;
  ASSERT_OK(copts.Serialize(config_options, &str));
  ASSERT_OK(copy.Configure(config_options, str));
  ASSERT_EQ(copy.local_sst_file_mode, LocalSstFileMode::kRemotePrimary);
  ASSERT_FALSE(copy.get_keep_local_sst_files());  // backwards compat
  ASSERT_FALSE(copy.keep_local_log_files);
  ASSERT_FALSE(copy.create_bucket_if_missing);
  ASSERT_FALSE(copy.validate_filesize);
  ASSERT_FALSE(copy.skip_dbid_verification);
  ASSERT_FALSE(copy.resync_on_open);
  ASSERT_FALSE(copy.skip_cloud_files_in_getchildren);
  ASSERT_FALSE(copy.run_purger);
  ASSERT_EQ(copy.constant_sst_file_size_in_sst_file_manager, 100);
  ASSERT_EQ(copy.purger_periodicity_millis, 101);

  // Now try kEagerMirror (legacy true)
  copts.local_sst_file_mode = LocalSstFileMode::kEagerMirror;
  copts.keep_local_log_files = true;
  copts.create_bucket_if_missing = true;
  copts.validate_filesize = true;
  copts.skip_dbid_verification = true;
  copts.resync_on_open = true;
  copts.skip_cloud_files_in_getchildren = true;
  copts.constant_sst_file_size_in_sst_file_manager = 200;
  copts.run_purger = true;
  copts.purger_periodicity_millis = 201;

  ASSERT_OK(copts.Serialize(config_options, &str));
  ASSERT_OK(copy.Configure(config_options, str));
  ASSERT_EQ(copy.local_sst_file_mode, LocalSstFileMode::kEagerMirror);
  ASSERT_TRUE(copy.get_keep_local_sst_files());  // backwards compat
  ASSERT_TRUE(copy.keep_local_log_files);
  ASSERT_TRUE(copy.create_bucket_if_missing);
  ASSERT_TRUE(copy.validate_filesize);
  ASSERT_TRUE(copy.skip_dbid_verification);
  ASSERT_TRUE(copy.resync_on_open);
  ASSERT_TRUE(copy.skip_cloud_files_in_getchildren);
  ASSERT_TRUE(copy.run_purger);
  ASSERT_EQ(copy.constant_sst_file_size_in_sst_file_manager, 200);
  ASSERT_EQ(copy.purger_periodicity_millis, 201);
}

TEST(CloudFileSystemTest, ConfigureAllSstModes) {
  ConfigOptions config_options;
  CloudFileSystemOptions copts, copy;
  std::string str;

  // Test all four modes round-trip through serialize/configure
  LocalSstFileMode modes[] = {
      LocalSstFileMode::kRemotePrimary,
      LocalSstFileMode::kEagerMirror,
      LocalSstFileMode::kCacheOnDemand,
      LocalSstFileMode::kTrickleSync,
  };
  for (auto mode : modes) {
    copts.local_sst_file_mode = mode;
    ASSERT_OK(copts.Serialize(config_options, &str));
    ASSERT_OK(copy.Configure(config_options, str));
    ASSERT_EQ(copy.local_sst_file_mode, mode);
  }
}

TEST(CloudFileSystemTest, LegacyBooleanAlias) {
  ConfigOptions config_options;
  config_options.invoke_prepare_options = false;

  // "keep_local_sst_files=true" should map to kEagerMirror
  std::unique_ptr<CloudFileSystem> cfs;
  ASSERT_OK(CloudFileSystemEnv::CreateFromString(
      config_options, "keep_local_sst_files=true", &cfs));
  auto copts = cfs->GetOptions<CloudFileSystemOptions>();
  ASSERT_EQ(copts->local_sst_file_mode, LocalSstFileMode::kEagerMirror);

  // "keep_local_sst_files=false" should map to kRemotePrimary
  ASSERT_OK(CloudFileSystemEnv::CreateFromString(
      config_options, "keep_local_sst_files=false", &cfs));
  copts = cfs->GetOptions<CloudFileSystemOptions>();
  ASSERT_EQ(copts->local_sst_file_mode, LocalSstFileMode::kRemotePrimary);
}

TEST(CloudFileSystemTest, HelperFunctions) {
  // kRemotePrimary: no local files
  ASSERT_FALSE(KeepsLocalSstFiles(LocalSstFileMode::kRemotePrimary));
  ASSERT_FALSE(SupportsMmapReads(LocalSstFileMode::kRemotePrimary));
  ASSERT_FALSE(DownloadsAllOnOpen(LocalSstFileMode::kRemotePrimary));

  // kEagerMirror: all local, mmap OK, downloads on open
  ASSERT_TRUE(KeepsLocalSstFiles(LocalSstFileMode::kEagerMirror));
  ASSERT_TRUE(SupportsMmapReads(LocalSstFileMode::kEagerMirror));
  ASSERT_TRUE(DownloadsAllOnOpen(LocalSstFileMode::kEagerMirror));

  // kCacheOnDemand: keeps local, no mmap, no bulk download
  ASSERT_TRUE(KeepsLocalSstFiles(LocalSstFileMode::kCacheOnDemand));
  ASSERT_FALSE(SupportsMmapReads(LocalSstFileMode::kCacheOnDemand));
  ASSERT_FALSE(DownloadsAllOnOpen(LocalSstFileMode::kCacheOnDemand));

  // kTrickleSync: keeps local, no mmap, no bulk download
  ASSERT_TRUE(KeepsLocalSstFiles(LocalSstFileMode::kTrickleSync));
  ASSERT_FALSE(SupportsMmapReads(LocalSstFileMode::kTrickleSync));
  ASSERT_FALSE(DownloadsAllOnOpen(LocalSstFileMode::kTrickleSync));
}

TEST(CloudFileSystemTest, ConfigureBucketOptions) {
  ConfigOptions config_options;
  CloudFileSystemOptions copts, copy;
  std::string str;
  copts.src_bucket.SetBucketName("source", "src.");
  copts.src_bucket.SetObjectPath("foo");
  copts.src_bucket.SetRegion("north");
  copts.dest_bucket.SetBucketName("dest");
  copts.dest_bucket.SetObjectPath("bar");
  ASSERT_OK(copts.Serialize(config_options, &str));

  ASSERT_OK(copy.Configure(config_options, str));
  ASSERT_EQ(copts.src_bucket.GetBucketName(), copy.src_bucket.GetBucketName());
  ASSERT_EQ(copts.src_bucket.GetObjectPath(), copy.src_bucket.GetObjectPath());
  ASSERT_EQ(copts.src_bucket.GetRegion(), copy.src_bucket.GetRegion());

  ASSERT_EQ(copts.dest_bucket.GetBucketName(),
            copy.dest_bucket.GetBucketName());
  ASSERT_EQ(copts.dest_bucket.GetObjectPath(),
            copy.dest_bucket.GetObjectPath());
  ASSERT_EQ(copts.dest_bucket.GetRegion(), copy.dest_bucket.GetRegion());
}

TEST(CloudFileSystemTest, ConfigureEnv) {
  std::unique_ptr<CloudFileSystem> cfs;

  ConfigOptions config_options;
  config_options.invoke_prepare_options = false;
  ASSERT_OK(CloudFileSystemEnv::CreateFromString(
      config_options, "local_sst_file_mode=kCacheOnDemand", &cfs));
  ASSERT_NE(cfs, nullptr);
  ASSERT_STREQ(cfs->Name(), "cloud");
  auto copts = cfs->GetOptions<CloudFileSystemOptions>();
  ASSERT_NE(copts, nullptr);
  ASSERT_EQ(copts->local_sst_file_mode, LocalSstFileMode::kCacheOnDemand);
}

TEST(CloudFileSystemTest, TestInitialize) {
  std::unique_ptr<CloudFileSystem> cfs;
  BucketOptions bucket;
  ConfigOptions config_options;
  config_options.invoke_prepare_options = false;
  ASSERT_OK(CloudFileSystemEnv::CreateFromString(
      config_options, "id=cloud; TEST=cloudenvtest:/test/path", &cfs));
  ASSERT_NE(cfs, nullptr);
  ASSERT_STREQ(cfs->Name(), "cloud");

  ASSERT_TRUE(StartsWith(cfs->GetSrcBucketName(),
                         bucket.GetBucketPrefix() + "cloudenvtest."));
  ASSERT_EQ(cfs->GetSrcObjectPath(), "/test/path");
  ASSERT_TRUE(cfs->SrcMatchesDest());

  ASSERT_OK(CloudFileSystemEnv::CreateFromString(
      config_options, "id=cloud; TEST=cloudenvtest2:/test/path2?here", &cfs));
  ASSERT_NE(cfs, nullptr);
  ASSERT_STREQ(cfs->Name(), "cloud");
  ASSERT_TRUE(StartsWith(cfs->GetSrcBucketName(),
                         bucket.GetBucketPrefix() + "cloudenvtest2."));
  ASSERT_EQ(cfs->GetSrcObjectPath(), "/test/path2");
  ASSERT_EQ(cfs->GetCloudFileSystemOptions().src_bucket.GetRegion(), "here");
  ASSERT_TRUE(cfs->SrcMatchesDest());

  ASSERT_OK(CloudFileSystemEnv::CreateFromString(
      config_options,
      "id=cloud; TEST=cloudenvtest3:/test/path3; "
      "src.bucket=my_bucket; dest.object=/my_path",
      &cfs));
  ASSERT_NE(cfs, nullptr);
  ASSERT_STREQ(cfs->Name(), "cloud");
  ASSERT_EQ(cfs->GetSrcBucketName(), bucket.GetBucketPrefix() + "my_bucket");
  ASSERT_EQ(cfs->GetSrcObjectPath(), "/test/path3");
  ASSERT_TRUE(StartsWith(cfs->GetDestBucketName(),
                         bucket.GetBucketPrefix() + "cloudenvtest3."));
  ASSERT_EQ(cfs->GetDestObjectPath(), "/my_path");
}

TEST(CloudFileSystemTest, ConfigureAwsEnv) {
  std::unique_ptr<CloudFileSystem> cfs;

  ConfigOptions config_options;
  Status s = CloudFileSystemEnv::CreateFromString(
      config_options, "id=aws; local_sst_file_mode=kEagerMirror", &cfs);
#ifdef USE_AWS
  ASSERT_OK(s);
  ASSERT_NE(cfs, nullptr);
  ASSERT_STREQ(cfs->Name(), "aws");
  auto copts = cfs->GetOptions<CloudFileSystemOptions>();
  ASSERT_NE(copts, nullptr);
  ASSERT_EQ(copts->local_sst_file_mode, LocalSstFileMode::kEagerMirror);
  ASSERT_NE(cfs->GetStorageProvider(), nullptr);
  ASSERT_STREQ(cfs->GetStorageProvider()->Name(),
               CloudStorageProviderImpl::kS3());
#else
  ASSERT_NOK(s);
  ASSERT_EQ(cfs, nullptr);
#endif
}

TEST(CloudFileSystemTest, ConfigureS3Provider) {
  std::unique_ptr<CloudFileSystem> cfs;

  ConfigOptions config_options;
  Status s =
      CloudFileSystemEnv::CreateFromString(config_options, "provider=s3", &cfs);
  ASSERT_NOK(s);
  ASSERT_EQ(cfs, nullptr);

#ifdef USE_AWS
  ASSERT_OK(CloudFileSystemEnv::CreateFromString(config_options,
                                              "id=aws; provider=s3", &cfs));
  ASSERT_STREQ(cfs->Name(), "aws");
  ASSERT_NE(cfs->GetStorageProvider(), nullptr);
  ASSERT_STREQ(cfs->GetStorageProvider()->Name(),
               CloudStorageProviderImpl::kS3());
#endif
}

// Test is disabled until we have a mock provider and authentication issues are
// resolved
TEST(CloudFileSystemTest, DISABLED_ConfigureKinesisController) {
  std::unique_ptr<CloudFileSystem> cfs;

  ConfigOptions config_options;
  Status s = CloudFileSystemEnv::CreateFromString(
      config_options, "provider=mock; controller=kinesis", &cfs);
  ASSERT_NOK(s);
  ASSERT_EQ(cfs, nullptr);

#ifdef USE_AWS
  ASSERT_OK(CloudFileSystemEnv::CreateFromString(
      config_options, "id=aws; controller=kinesis; TEST=dbcloud:/test", &cfs));
  ASSERT_STREQ(cfs->Name(), "aws");
  ASSERT_NE(cfs->GetCloudFileSystemOptions().cloud_log_controller, nullptr);
  ASSERT_STREQ(cfs->GetCloudFileSystemOptions().cloud_log_controller->Name(),
               CloudLogControllerImpl::kKinesis());
#endif
}

TEST(CloudFileSystemTest, ConfigureKafkaController) {
  std::unique_ptr<CloudFileSystem> cfs;

  ConfigOptions config_options;
  Status s = CloudFileSystemEnv::CreateFromString(
      config_options, "provider=mock; controller=kafka", &cfs);
#ifdef USE_KAFKA
  ASSERT_OK(s);
  ASSERT_NE(cfs, nullptr);
  ASSERT_NE(cfs->GetCloudFileSystemOptions().cloud_log_controller, nullptr);
  ASSERT_STREQ(cfs->GetCloudFileSystemOptions().cloud_log_controller->Name(),
               CloudLogControllerImpl::kKafka());
#else
  ASSERT_NOK(s);
  ASSERT_EQ(cfs, nullptr);
#endif
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
