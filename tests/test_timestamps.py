"""Run with: python -m unittest discover -s tests -v."""

import importlib.util
import io
import os
from contextlib import redirect_stdout
from datetime import datetime
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import pandas as pd
import piexif
from PIL import Image


SOURCE = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SOURCE))
SPEC = importlib.util.spec_from_file_location(
    "update_output_table", SOURCE / "3_update_output_table.py"
)
update_table = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(update_table)


class TimestampTests(unittest.TestCase):
    EXIF_DATE = b"2024:02:29 12:34:56"
    EXIF_TIMESTAMP = "2024-02-29 12:34:56"
    MTIME = 1_600_000_000

    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.directory = Path(self.temporary_directory.name)
        self.mtime_timestamp = datetime.fromtimestamp(self.MTIME).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    def make_jpeg(self, path, exif=None):
        options = {"exif": exif} if exif is not None else {}
        Image.new("RGB", (2, 2)).save(path, "JPEG", **options)
        os.utime(path, (self.MTIME, self.MTIME))
        return path

    def timestamp(self, path):
        with redirect_stdout(io.StringIO()):
            return update_table.extract_timestamp(path)

    def test_jpeg_exif_takes_precedence_over_mtime(self):
        exif = piexif.dump(
            {"Exif": {piexif.ExifIFD.DateTimeOriginal: self.EXIF_DATE}}
        )
        for filename in ("camera.JPG", "camera.jpeg", "camera.JPEG"):
            with self.subTest(filename=filename):
                path = self.make_jpeg(self.directory / filename, exif)
                for argument in (path, str(path)):
                    self.assertEqual(self.timestamp(argument), self.EXIF_TIMESTAMP)

    def test_jpeg_without_exif_falls_back_to_mtime(self):
        path = self.make_jpeg(self.directory / "no_exif.jpg")
        self.assertEqual(self.timestamp(path), self.mtime_timestamp)

    def test_exif_without_original_date_falls_back_to_mtime(self):
        exif = piexif.dump(
            {"Exif": {piexif.ExifIFD.DateTimeDigitized: self.EXIF_DATE}}
        )
        path = self.make_jpeg(self.directory / "no_original_date.jpg", exif)
        self.assertEqual(self.timestamp(path), self.mtime_timestamp)

    def test_invalid_exif_date_falls_back_to_mtime(self):
        for date in (b"not a date", b"2024:02:30 12:34:56", b"\xff"):
            with self.subTest(date=date):
                exif = piexif.dump(
                    {"Exif": {piexif.ExifIFD.DateTimeOriginal: date}}
                )
                path = self.make_jpeg(self.directory / "bad_date.jpg", exif)
                self.assertEqual(self.timestamp(path), self.mtime_timestamp)

    def test_malformed_exif_falls_back_to_mtime(self):
        path = self.make_jpeg(
            self.directory / "bad_exif.jpg", b"Exif\x00\x00broken"
        )
        self.assertEqual(self.timestamp(path), self.mtime_timestamp)

    def test_disguised_non_jpeg_files_never_enter_other_parsers(self):
        # Small, valid files exercise dispatch without running exploit payloads.
        Image.init()
        for image_format in ("PNG", "APNG", "GIF", "XBM"):
            with self.subTest(image_format=image_format):
                path = self.directory / f"disguised_{image_format}.jpg"
                options = {}
                actual_format = image_format
                if image_format in ("PNG", "APNG"):
                    options["exif"] = piexif.dump(
                        {"Exif": {piexif.ExifIFD.DateTimeOriginal: self.EXIF_DATE}}
                    )
                if image_format == "APNG":
                    actual_format = "PNG"
                    options.update(
                        save_all=True,
                        append_images=[Image.new("1", (2, 2), 1)],
                        duration=100,
                        loop=0,
                    )
                Image.new("1", (2, 2)).save(path, actual_format, **options)
                os.utime(path, (self.MTIME, self.MTIME))
                factory, accept = Image.OPEN[actual_format]
                factory_spy = Mock(wraps=factory)
                with patch.dict(Image.OPEN, {actual_format: (factory_spy, accept)}):
                    result = self.timestamp(path)
                factory_spy.assert_not_called()
                self.assertEqual(result, self.mtime_timestamp)

    def test_corrupt_image_falls_back_to_mtime(self):
        path = self.directory / "corrupt.jpg"
        path.write_bytes(b"not an image")
        os.utime(path, (self.MTIME, self.MTIME))
        self.assertEqual(self.timestamp(path), self.mtime_timestamp)

    def test_missing_file_returns_na(self):
        self.assertEqual(self.timestamp(self.directory / "missing.jpg"), "NA")

    def test_failed_mtime_lookup_returns_na(self):
        path = self.make_jpeg(self.directory / "no_exif.jpg")
        with patch.object(
            update_table.os.path, "getmtime", side_effect=PermissionError("denied")
        ):
            self.assertEqual(self.timestamp(path), "NA")

    def test_recovered_images_retain_timestamps_in_reconciled_table(self):
        species_directory = self.directory / "site_a" / "animal" / "wallaby"
        species_directory.mkdir(parents=True)
        exif = piexif.dump(
            {"Exif": {piexif.ExifIFD.DateTimeOriginal: self.EXIF_DATE}}
        )
        self.make_jpeg(species_directory / "recovered.JPG", exif)
        self.make_jpeg(species_directory / "fallback.jpeg")
        original = pd.DataFrame(
            [{
                "camera_site": "site_a",
                "filename": "existing.jpg",
                "class_name": "wallaby",
                "class_id": 7,
                "timestamp": "01/01/2020 00:00:00",
                "expert_updated": 0,
            }]
        )
        with redirect_stdout(io.StringIO()):
            mapping = update_table.scan_animal_folders(self.directory)
            result = update_table.reconcile_table(original, mapping)
        rows = result.set_index("filename")
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows.at["existing.jpg", "timestamp"], "01/01/2020 00:00:00")
        self.assertEqual(rows.at["recovered.JPG", "timestamp"], "29/02/2024 12:34:56")
        self.assertEqual(
            rows.at["fallback.jpeg", "timestamp"],
            datetime.fromtimestamp(self.MTIME).strftime("%d/%m/%Y %H:%M:%S"),
        )
        self.assertEqual(rows.at["recovered.JPG", "expert_updated"], 4)
        self.assertEqual(rows.at["fallback.jpeg", "expert_updated"], 4)


if __name__ == "__main__":
    unittest.main()
