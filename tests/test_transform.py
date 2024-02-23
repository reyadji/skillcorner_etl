import unittest
import pandas as pd
import etl


TEST_TRACKING_FILE = "test_tracking.txt"
TEST_GAME_ID = 123456


class TestETL(unittest.TestCase):
    def setUp(self):
        self.test_etl = etl.TrackingEtl()
        self.test_etl.tracking_df = pd.read_json(TEST_TRACKING_FILE, lines=True)
        self.test_etl.tracking_df["game_id"] = TEST_GAME_ID

    def test_transform_tracking(self):
        self.test_etl.transform_tracking()
        expected_track_output = pd.DataFrame(
            {
                "game_id": [TEST_GAME_ID],
                "frame": [10],
                "track_id": [55],
                "trackable_object": ["55"],
                "is_visible": [False],
                "x": [-3.13],
                "y": [-4.92],
                "z": [-0.31],
            }
        )
        expected_possession_output = pd.DataFrame(
            {
                "game_id": [TEST_GAME_ID],
                "group": [None],
                "trackable_object": ["<NA>"],
            }
        )
        expected_frame_output = pd.DataFrame(
            {
                "game_id": [TEST_GAME_ID],
                "frame": [10],
                "image_corners_projection": ["[]"],
                "period": [1],
                "timestamp_in_seconds": [0],
            }
        )
        pd.testing.assert_frame_equal(self.test_etl.track_df, expected_track_output)
        pd.testing.assert_frame_equal(
            self.test_etl.possession_df, expected_possession_output
        )
        pd.testing.assert_frame_equal(self.test_etl.frame_df, expected_frame_output)


if __name__ == "__main__":
    unittest.main()
