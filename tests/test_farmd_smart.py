"""Tests for farmd smart alert integration (farmd smart record/list/clear)."""
import pytest

from farmfs import getvol
from farmfs.farmd import JobRunner, SmartAlert, decode_smart_alert, encode_smart_alert
from farmfs.farmd_ui import farmd_ui
from farmfs.fs import Path
from farmfs.volume import FarmFSVolume, mkfs


# ── Fixtures ──────────────────────────────────────────────────────────────────

def make_volume(path: Path) -> FarmFSVolume:
    udd_path = path.join(".farmfs").join("userdata")
    mkfs(path, udd_path)
    return getvol(path)


@pytest.fixture
def farmd_vol(tmp: Path) -> Path:
    vol_path = tmp.join("farmd")
    make_volume(vol_path)
    return vol_path


def _jr(vol_path: Path) -> JobRunner:
    return JobRunner(getvol(vol_path))


# ── SmartAlert encode/decode round-trips ─────────────────────────────────────

def test_smart_alert_roundtrip() -> None:
    a = SmartAlert(
        device="/dev/sda",
        fail_type="Health",
        message="Device failure: /dev/sda",
        full_message="SMART overall-health self-assessment test result: FAILED!",
        device_info="WDC WD20EZRZ-00 [WD-WCC4N5JY1234]",
        received_at="2026-03-04T12:00:00+00:00",
        prevcnt=0,
    )
    encoded = encode_smart_alert(a)
    decoded = decode_smart_alert(encoded, "dev_sda")
    assert decoded == a


def test_smart_alert_roundtrip_defaults() -> None:
    d = {"device": "/dev/sdb"}
    a = decode_smart_alert(d, "dev_sdb")
    assert a.device == "/dev/sdb"
    assert a.fail_type == ""
    assert a.message == ""
    assert a.full_message == ""
    assert a.device_info == ""
    assert a.received_at == ""
    assert a.prevcnt == 0


# ── farmd smart record ────────────────────────────────────────────────────────

def test_smart_record_missing_device_env(farmd_vol: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SMARTD_DEVICE", raising=False)
    rc = farmd_ui(["smart", "record"], farmd_vol)
    assert rc == 1


def test_smart_record_writes_alert(farmd_vol: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SMARTD_DEVICE", "/dev/sda")
    monkeypatch.setenv("SMARTD_FAILTYPE", "Health")
    monkeypatch.setenv("SMARTD_MESSAGE", "Device failure: /dev/sda")
    monkeypatch.setenv("SMARTD_FULLMESSAGE", "Full report text here.")
    monkeypatch.setenv("SMARTD_DEVICEINFO", "WDC WD20EZRZ-00 [WD-WCC4N5JY1234]")
    monkeypatch.setenv("SMARTD_PREVCNT", "2")

    rc = farmd_ui(["smart", "record"], farmd_vol)
    assert rc == 0

    jr = _jr(farmd_vol)
    a = jr.smartdb.read("dev_sda")
    assert a.device == "/dev/sda"
    assert a.fail_type == "Health"
    assert a.message == "Device failure: /dev/sda"
    assert a.full_message == "Full report text here."
    assert a.device_info == "WDC WD20EZRZ-00 [WD-WCC4N5JY1234]"
    assert a.prevcnt == 2
    assert a.received_at != ""


def test_smart_record_overwrites_existing(farmd_vol: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Second alert for the same device replaces the first."""
    for prevcnt in ("0", "1"):
        monkeypatch.setenv("SMARTD_DEVICE", "/dev/sda")
        monkeypatch.setenv("SMARTD_FAILTYPE", "ErrorCount")
        monkeypatch.setenv("SMARTD_MESSAGE", "ATA error count increased")
        monkeypatch.setenv("SMARTD_FULLMESSAGE", "")
        monkeypatch.setenv("SMARTD_DEVICEINFO", "")
        monkeypatch.setenv("SMARTD_PREVCNT", prevcnt)
        farmd_ui(["smart", "record"], farmd_vol)

    jr = _jr(farmd_vol)
    a = jr.smartdb.read("dev_sda")
    assert a.prevcnt == 1  # last write wins


def test_smart_record_device_key_sanitisation(farmd_vol: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Device paths with slashes are sanitised into a valid key."""
    monkeypatch.setenv("SMARTD_DEVICE", "/dev/disk/by-id/ata-WDC_WD20")
    monkeypatch.setenv("SMARTD_FAILTYPE", "Health")
    monkeypatch.setenv("SMARTD_MESSAGE", "Failure")
    monkeypatch.setenv("SMARTD_FULLMESSAGE", "")
    monkeypatch.setenv("SMARTD_DEVICEINFO", "")
    monkeypatch.setenv("SMARTD_PREVCNT", "0")

    rc = farmd_ui(["smart", "record"], farmd_vol)
    assert rc == 0

    jr = _jr(farmd_vol)
    keys = jr.smartdb.list()
    assert len(keys) == 1
    # Key must not contain slashes
    assert "/" not in keys[0]


# ── farmd smart list ──────────────────────────────────────────────────────────

def test_smart_list_empty(farmd_vol: Path, capsys: pytest.CaptureFixture) -> None:
    rc = farmd_ui(["smart", "list"], farmd_vol)
    assert rc == 0
    out = capsys.readouterr().out
    assert "No smart alerts" in out


def test_smart_list_shows_alert(farmd_vol: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    monkeypatch.setenv("SMARTD_DEVICE", "/dev/sda")
    monkeypatch.setenv("SMARTD_FAILTYPE", "Health")
    monkeypatch.setenv("SMARTD_MESSAGE", "Device failing")
    monkeypatch.setenv("SMARTD_FULLMESSAGE", "")
    monkeypatch.setenv("SMARTD_DEVICEINFO", "")
    monkeypatch.setenv("SMARTD_PREVCNT", "0")
    farmd_ui(["smart", "record"], farmd_vol)
    capsys.readouterr()

    rc = farmd_ui(["smart", "list"], farmd_vol)
    assert rc == 0
    out = capsys.readouterr().out
    assert "/dev/sda" in out
    assert "Health" in out
    assert "Device failing" in out


# ── farmd smart clear ─────────────────────────────────────────────────────────

def test_smart_clear_removes_alert(farmd_vol: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SMARTD_DEVICE", "/dev/sda")
    monkeypatch.setenv("SMARTD_FAILTYPE", "Health")
    monkeypatch.setenv("SMARTD_MESSAGE", "Disk dying")
    monkeypatch.setenv("SMARTD_FULLMESSAGE", "")
    monkeypatch.setenv("SMARTD_DEVICEINFO", "")
    monkeypatch.setenv("SMARTD_PREVCNT", "0")
    farmd_ui(["smart", "record"], farmd_vol)

    rc = farmd_ui(["smart", "clear", "/dev/sda"], farmd_vol)
    assert rc == 0

    jr = _jr(farmd_vol)
    assert jr.smartdb.list() == []


def test_smart_clear_missing(farmd_vol: Path) -> None:
    rc = farmd_ui(["smart", "clear", "/dev/sda"], farmd_vol)
    assert rc == 1


# ── farmd status shows device health ─────────────────────────────────────────

def test_status_no_smart_alerts(farmd_vol: Path, capsys: pytest.CaptureFixture) -> None:
    from unittest.mock import patch
    with patch("farmfs.farmd_ui.check_daemon", return_value=("stopped", None)):
        rc = farmd_ui(["status"], farmd_vol)
    assert rc == 0
    out = capsys.readouterr().out
    # No device health section when there are no alerts
    assert "FAIL TYPE" not in out


def test_status_shows_smart_alerts(farmd_vol: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    from unittest.mock import patch
    monkeypatch.setenv("SMARTD_DEVICE", "/dev/sdb")
    monkeypatch.setenv("SMARTD_FAILTYPE", "ErrorCount")
    monkeypatch.setenv("SMARTD_MESSAGE", "ATA error count increased to 5")
    monkeypatch.setenv("SMARTD_FULLMESSAGE", "")
    monkeypatch.setenv("SMARTD_DEVICEINFO", "")
    monkeypatch.setenv("SMARTD_PREVCNT", "0")
    farmd_ui(["smart", "record"], farmd_vol)
    capsys.readouterr()

    with patch("farmfs.farmd_ui.check_daemon", return_value=("stopped", None)):
        rc = farmd_ui(["status"], farmd_vol)
    assert rc == 0
    out = capsys.readouterr().out
    assert "/dev/sdb" in out
    assert "ErrorCount" in out
    assert "ATA error count increased to 5" in out
