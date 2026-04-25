import errno
import json
import subprocess
from unittest.mock import MagicMock, patch

import pytest

import inner
from inner import (
    _read_key_file,
    get_dir_bytes,
    get_posix_export_dirs,
    get_required_config,
    get_s3_export_buckets,
    handle_posix,
    handle_s3,
    main,
)

# ---------------------------------------------------------------------------
# get_posix_export_dirs
# ---------------------------------------------------------------------------


class TestGetPosixExportDirs:
    def test_new_style_public(self):
        # New-style config with PublicReads capability should parse correctly
        # and mark the export as public
        cfg = {
            "Exports": [
                {
                    "storageprefix": "/data",
                    "federationprefix": "/ospool/data",
                    "capabilities": ["PublicReads", "Writes"],
                }
            ]
        }
        exports = get_posix_export_dirs(cfg)
        assert len(exports) == 1
        assert exports[0].storage_prefix == "/data"
        assert exports[0].federation_prefix == "/ospool/data"
        assert exports[0].public is True

    def test_new_style_authenticated(self):
        # New-style config without PublicReads should parse correctly
        # and mark the export as authenticated (not public)
        cfg = {
            "Exports": [
                {
                    "storageprefix": "/private",
                    "federationprefix": "/ospool/private",
                    "capabilities": ["Reads"],
                }
            ]
        }
        exports = get_posix_export_dirs(cfg)
        assert len(exports) == 1
        assert exports[0].public is False

    def test_new_style_malformed_skipped(self):
        # Malformed entries (missing required fields) should be skipped
        # Only well-formed entries should be returned
        cfg = {
            "Exports": [
                {
                    "storageprefix": "/broken"
                },  # missing federationprefix and capabilities
                {
                    "storageprefix": "/ok",
                    "federationprefix": "/ospool/ok",
                    "capabilities": [],
                },
            ]
        }
        exports = get_posix_export_dirs(cfg)
        assert len(exports) == 1
        assert exports[0].storage_prefix == "/ok"

    def test_old_style_public(self):
        # Legacy ExportVolumes format with EnablePublicReads flag
        # should parse correctly and mark as public
        cfg = {
            "ExportVolumes": ["/data:/ospool/data"],
            "EnablePublicReads": True,
        }
        exports = get_posix_export_dirs(cfg)
        assert len(exports) == 1
        assert exports[0].storage_prefix == "/data"
        assert exports[0].federation_prefix == "/ospool/data"
        assert exports[0].public is True

    def test_old_style_authenticated(self):
        # Legacy format without EnablePublicReads should default to authenticated
        cfg = {"ExportVolumes": ["/data:/ospool/data"]}
        exports = get_posix_export_dirs(cfg)
        assert len(exports) == 1
        assert exports[0].public is False

    def test_old_style_malformed_too_many_colons(self):
        # Volume format must be exactly storage:federation, no extra colons
        cfg = {"ExportVolumes": ["/data:/ospool/data:/extra"]}
        assert get_posix_export_dirs(cfg) == []

    def test_old_style_malformed_non_absolute_storage(self):
        # Storage path must be absolute
        cfg = {"ExportVolumes": ["data:/ospool/data"]}
        assert get_posix_export_dirs(cfg) == []

    def test_old_style_malformed_non_absolute_federation(self):
        # Federation path must be absolute
        cfg = {"ExportVolumes": ["/data:ospool/data"]}
        assert get_posix_export_dirs(cfg) == []

    def test_both_styles_merged(self):
        # Config with both old and new style should parse both correctly
        # and return combined exports
        cfg = {
            "Exports": [
                {
                    "storageprefix": "/new",
                    "federationprefix": "/ospool/new",
                    "capabilities": ["PublicReads"],
                }
            ],
            "ExportVolumes": ["/old:/ospool/old"],
        }
        exports = get_posix_export_dirs(cfg)
        assert len(exports) == 2
        prefixes = {e.storage_prefix for e in exports}
        assert prefixes == {"/new", "/old"}

    def test_empty_config(self):
        # Empty config should return empty list of exports
        assert get_posix_export_dirs({}) == []


# ---------------------------------------------------------------------------
# get_s3_export_buckets
# ---------------------------------------------------------------------------


class TestGetS3ExportBuckets:
    def test_public(self):
        # S3 export with PublicReads capability should be marked as public
        cfg = {
            "Exports": [
                {
                    "s3bucket": "my-bucket",
                    "federationprefix": "/ospool/bucket",
                    "capabilities": ["PublicReads"],
                }
            ]
        }
        exports = get_s3_export_buckets(cfg)
        assert len(exports) == 1
        assert exports[0].s3bucket == "my-bucket"
        assert exports[0].federation_prefix == "/ospool/bucket"
        assert exports[0].public is True

    def test_authenticated(self):
        # S3 export without PublicReads should be authenticated
        cfg = {
            "Exports": [
                {
                    "s3bucket": "private-bucket",
                    "federationprefix": "/ospool/private",
                    "capabilities": [],
                }
            ]
        }
        exports = get_s3_export_buckets(cfg)
        assert len(exports) == 1
        assert exports[0].public is False

    def test_malformed_missing_s3bucket_skipped(self):
        # Exports missing required s3bucket field should be skipped
        cfg = {"Exports": [{"federationprefix": "/ospool/x", "capabilities": []}]}
        assert get_s3_export_buckets(cfg) == []

    def test_malformed_missing_federationprefix_skipped(self):
        # Exports missing required federationprefix field should be skipped
        cfg = {"Exports": [{"s3bucket": "bucket", "capabilities": []}]}
        assert get_s3_export_buckets(cfg) == []

    def test_empty_exports(self):
        # Empty or missing Exports should return empty list
        assert get_s3_export_buckets({}) == []
        assert get_s3_export_buckets({"Exports": []}) == []


# ---------------------------------------------------------------------------
# get_dir_bytes
# ---------------------------------------------------------------------------


class TestGetDirBytes:
    def test_cephfs_fast_path(self):
        # CephFS fast path uses getxattr to get directory size
        with patch("os.getxattr", return_value=b"123456789"):
            result = get_dir_bytes("/some/path")
        assert result == 123456789

    def test_enodata_falls_through_to_du(self):
        # ENODATA errno (no extended attribute) falls back to du command
        err = OSError()
        err.errno = errno.ENODATA
        mock_proc = MagicMock()
        mock_proc.stdout = "9876\t/some/path\n"
        with patch("os.getxattr", side_effect=err):
            with patch("subprocess.run", return_value=mock_proc) as mock_run:
                result = get_dir_bytes("/some/path")
        assert result == 9876
        mock_run.assert_called_once()

    def test_eopnotsupp_falls_through_to_du(self):
        # EOPNOTSUPP errno (operation not supported) falls back to du command
        err = OSError()
        err.errno = errno.EOPNOTSUPP
        mock_proc = MagicMock()
        mock_proc.stdout = "42\t/some/path\n"
        with patch("os.getxattr", side_effect=err):
            with patch("subprocess.run", return_value=mock_proc):
                result = get_dir_bytes("/some/path")
        assert result == 42

    def test_eacces_reraised(self):
        # Permission errors should be re-raised to caller
        err = OSError()
        err.errno = errno.EACCES
        with patch("os.getxattr", side_effect=err):
            with pytest.raises(OSError) as exc_info:
                get_dir_bytes("/no/permission")
        assert exc_info.value.errno == errno.EACCES

    def test_enoent_reraised(self):
        # File not found errors should be re-raised to caller
        err = OSError()
        err.errno = errno.ENOENT
        with patch("os.getxattr", side_effect=err):
            with pytest.raises(OSError) as exc_info:
                get_dir_bytes("/does/not/exist")
        assert exc_info.value.errno == errno.ENOENT


# ---------------------------------------------------------------------------
# _read_key_file
# ---------------------------------------------------------------------------


class TestReadKeyFile:
    def test_file_present_returns_stripped_content(self, tmp_path):
        # File content should be stripped of leading/trailing whitespace
        key_file = tmp_path / "access.key"
        key_file.write_text("  my-secret-key  \n")
        assert _read_key_file(str(key_file), "access key") == "my-secret-key"

    def test_file_missing_returns_none(self, tmp_path, capsys):
        # Missing key file should return None and print error message
        result = _read_key_file(str(tmp_path / "missing.key"), "access key")
        assert result is None
        assert "Error reading" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# handle_posix
# ---------------------------------------------------------------------------


class TestHandlePosix:
    _cfg_one = {
        "Exports": [
            {
                "storageprefix": "/data",
                "federationprefix": "/ospool/data",
                "capabilities": [],
            }
        ]
    }

    def test_size_populated_on_success(self):
        # Successful get_dir_bytes should populate size and leave error as None
        result = {"posix": {"exports": []}}
        with patch("inner.get_dir_bytes", return_value=100):
            handle_posix(self._cfg_one, result)
        exports = result["posix"]["exports"]
        assert len(exports) == 1
        assert exports[0]["size"] == 100
        assert exports[0]["error"] is None

    def test_per_export_error_does_not_stop_others(self):
        # If one export fails, other exports should still be processed successfully
        cfg = {
            "Exports": [
                {
                    "storageprefix": "/bad",
                    "federationprefix": "/ospool/bad",
                    "capabilities": [],
                },
                {
                    "storageprefix": "/good",
                    "federationprefix": "/ospool/good",
                    "capabilities": [],
                },
            ]
        }
        result = {"posix": {"exports": []}}

        def fake_get_dir_bytes(path):
            if path == "/bad":
                raise PermissionError("denied")
            return 999

        with patch("inner.get_dir_bytes", side_effect=fake_get_dir_bytes):
            handle_posix(cfg, result)

        by_prefix = {e["storage_prefix"]: e for e in result["posix"]["exports"]}
        assert by_prefix["/bad"]["size"] is None
        assert by_prefix["/bad"]["error"] == "denied"
        assert by_prefix["/good"]["size"] == 999
        assert by_prefix["/good"]["error"] is None


# ---------------------------------------------------------------------------
# handle_s3
# ---------------------------------------------------------------------------


class TestHandleS3:
    def _make_result(self):
        # Helper to create empty S3 result structure
        return {
            "s3": {
                "serviceurl": None,
                "region": None,
                "accesskey": None,
                "secretkey": None,
                "exports": [],
            }
        }

    def test_all_fields_populated(self, tmp_path):
        # All S3 fields should be populated correctly from config and key files
        access_key_file = tmp_path / "access.key"
        secret_key_file = tmp_path / "secret.key"
        access_key_file.write_text("AKID123")
        secret_key_file.write_text("SECRET456")

        cfg = {
            "S3ServiceURL": "https://s3.example.com",
            "S3Region": "us-east-1",
            "S3AccessKeyFile": str(access_key_file),
            "S3SecretKeyFile": str(secret_key_file),
            "Exports": [
                {
                    "s3bucket": "my-bucket",
                    "federationprefix": "/ospool/bucket",
                    "capabilities": ["PublicReads"],
                }
            ],
        }
        result = self._make_result()
        handle_s3(cfg, result)
        assert result["s3"]["serviceurl"] == "https://s3.example.com"
        assert result["s3"]["region"] == "us-east-1"
        assert result["s3"]["accesskey"] == "AKID123"
        assert result["s3"]["secretkey"] == "SECRET456"
        assert len(result["s3"]["exports"]) == 1

    def test_missing_key_files_leaves_keys_none(self):
        # Missing key files should leave access/secret keys as None, but other fields should populate
        cfg = {"S3ServiceURL": "https://s3.example.com", "S3Region": "us-east-1"}
        result = self._make_result()
        handle_s3(cfg, result)
        assert result["s3"]["accesskey"] is None
        assert result["s3"]["secretkey"] is None
        assert result["s3"]["serviceurl"] == "https://s3.example.com"


# ---------------------------------------------------------------------------
# get_required_config
# ---------------------------------------------------------------------------


class TestGetRequiredConfig:
    def test_valid_config(self):
        # Valid config should extract Origin, Sitename, and StorageType correctly
        fake_config = {
            "Xrootd": {"Sitename": "my-site"},
            "Origin": {"StorageType": "posix", "Exports": []},
        }
        with patch("inner.get_config", return_value=fake_config):
            origin_cfg, sitename, storagetype = get_required_config()
        assert sitename == "my-site"
        assert storagetype == "posix"
        assert origin_cfg == fake_config["Origin"]

    def test_missing_origin_raises(self):
        # Config without Origin section should raise error
        with patch("inner.get_config", return_value={"Xrootd": {"Sitename": "x"}}):
            with pytest.raises(RuntimeError, match="Origin config not found"):
                get_required_config()

    def test_missing_storagetype_raises(self):
        # Origin config without StorageType should raise error
        fake_config = {"Origin": {"Exports": []}}
        with patch("inner.get_config", return_value=fake_config):
            with pytest.raises(RuntimeError, match="StorageType not found"):
                get_required_config()


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

# Test constants for different storage types
# Used to mock different get_required_config returns in main() tests
_POSIX_CONFIG = (
    {"StorageType": "posix", "Exports": []},
    "my-site",
    "posix",
)

_S3_CONFIG = (
    {"StorageType": "s3", "Exports": []},
    "s3-site",
    "s3",
)


class TestMain:
    def test_posix_path_returns_0_and_emits_json(self, capsys):
        # POSIX path should return 0 and output JSON with correct fields
        with patch("inner.get_required_config", return_value=_POSIX_CONFIG):
            with patch("inner.handle_posix") as mock_hp:
                rc = main()
        assert rc == 0
        mock_hp.assert_called_once()
        data = json.loads(capsys.readouterr().out)
        assert data["status"] == "ok"
        assert data["storagetype"] == "posix"
        assert data["sitename"] == "my-site"

    def test_s3_path_calls_handle_s3(self, capsys):
        # S3 path should call handle_s3 handler
        with patch("inner.get_required_config", return_value=_S3_CONFIG):
            with patch("inner.handle_s3") as mock_hs:
                rc = main()
        assert rc == 0
        mock_hs.assert_called_once()

    def test_config_called_process_error_returns_1(self, capsys):
        # CalledProcessError from config retrieval should return 1 with error JSON
        err = subprocess.CalledProcessError(1, "pelican", stderr="config failed")
        with patch("inner.get_required_config", side_effect=err):
            rc = main()
        assert rc == 1
        data = json.loads(capsys.readouterr().out)
        assert data["status"] == "error"
        assert "config command failed" in data["error"]

    def test_config_generic_error_returns_1(self, capsys):
        # RuntimeError from config retrieval should return 1 with error JSON
        with patch("inner.get_required_config", side_effect=RuntimeError("bad config")):
            rc = main()
        assert rc == 1
        data = json.loads(capsys.readouterr().out)
        assert data["status"] == "error"
        assert "bad config" in data["error"]

    def test_handle_posix_error_returns_1(self, capsys):
        # RuntimeError in handle_posix should return 1 with error JSON
        with patch("inner.get_required_config", return_value=_POSIX_CONFIG):
            with patch("inner.handle_posix", side_effect=RuntimeError("disk error")):
                rc = main()
        assert rc == 1
        data = json.loads(capsys.readouterr().out)
        assert data["status"] == "error"

    def test_json_always_emitted_on_error(self, capsys):
        # Any error should always emit JSON with status, time, and error fields
        with patch("inner.get_required_config", side_effect=Exception("boom")):
            main()
        data = json.loads(capsys.readouterr().out)
        assert "time" in data
        assert data["status"] == "error"
