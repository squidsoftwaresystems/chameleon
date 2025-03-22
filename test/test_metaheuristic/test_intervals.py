from typing import List

import pandas as pd
import pytest

from src.metaheuristic.intervals import Intervals


def compare_intervals(a: Intervals, b: Intervals) -> bool:
    data1 = a.data
    # make the column order the same
    data2 = b.data[data1.columns]
    # make the index the same so that it can be compared
    return (
        (
            data1.sort_values("start_time").reset_index(drop=True)
            == data2.sort_values("start_time").reset_index(drop=True)
        )
        .all()
        .all()
    )


def test_from_row():
    """
    This function and test_from_list() also test Intervals.__sort_and_assert_valid()
    """

    # require start and end times
    with pytest.raises(AssertionError):
        Intervals.from_row(
            pd.Series({"start_time": pd.Timestamp(0), "test": 3})
        )
    with pytest.raises(AssertionError):
        Intervals.from_row(pd.Series({"end_time": pd.Timestamp(0), "test": 3}))

    # Require correct type of start_time, end_time
    with pytest.raises(AssertionError):
        Intervals.from_row(
            pd.Series(
                {"start_time": pd.Timestamp(0), "end_time": int, "test": 3}
            )
        )
    with pytest.raises(AssertionError):
        Intervals.from_row(
            pd.Series(
                {"start_time": int, "end_time": pd.Timestamp(0), "test": 3}
            )
        )

    series_data = {
        "start_time": pd.Timestamp(0),
        "end_time": pd.Timestamp(1),
        "info": 3,
        "other_info": "test",
    }
    single_interval = Intervals.from_row(pd.Series(series_data))

    # Can only be one column
    assert single_interval.data.shape[0]
    assert (single_interval.data.iloc[0] == pd.Series(series_data)).all()


def test_from_list():
    """
    This function and test_from_row() also test Intervals.__sort_and_assert_valid()
    """
    columns = ["num", "string"]

    # Note: these overlap
    val1_interval1 = (pd.Timestamp(0), pd.Timestamp(2), (3, "test"))
    val1_interval2 = (pd.Timestamp(1), pd.Timestamp(3), (3, "test"))

    val2_interval1 = (pd.Timestamp(0), pd.Timestamp(2), (4, "test"))
    val2_interval2 = (pd.Timestamp(2), pd.Timestamp(4), (4, "test"))
    val2_interval3 = (pd.Timestamp(5), pd.Timestamp(7), (4, "test"))
    # Note: overlaps
    val2_interval4 = (pd.Timestamp(4), pd.Timestamp(8), (4, "test"))

    # Overlaps
    with pytest.raises(AssertionError):
        Intervals.from_list(
            [val1_interval1, val1_interval2, val2_interval1],
            column_names=columns,
        )
    with pytest.raises(AssertionError):
        Intervals.from_list(
            [
                val1_interval1,
                val2_interval1,
                val2_interval2,
                val2_interval3,
                val2_interval4,
            ],
            column_names=columns,
        )

    interval_list = [
        val1_interval1,
        val2_interval1,
        val2_interval2,
        val2_interval3,
    ]
    intervals = Intervals.from_list(
        interval_list,
        column_names=columns,
    )

    # Make sure we have the correct data, sorted
    assert (
        (
            intervals.data.sort_values("start_time")
            == pd.DataFrame(
                {
                    "start_time": [val[0] for val in interval_list],
                    "end_time": [val[1] for val in interval_list],
                    columns[0]: [val[2][0] for val in interval_list],
                    columns[1]: [val[2][1] for val in interval_list],
                }
            ).sort_values("start_time")
        )
        .all()
        .all()
    )


def test_extract_interval():
    intervals = Intervals.from_list(
        [
            (pd.Timestamp(0), pd.Timestamp(4), (3, 4)),
            (pd.Timestamp(5), pd.Timestamp(7), (4, 4)),
            (pd.Timestamp(7), pd.Timestamp(10), (3, 5)),
            (pd.Timestamp(11), pd.Timestamp(13), (3, 5)),
        ],
        column_names=["test1", "test2"],
    )

    # make an illegal interval that overlaps with others
    intervals.data = pd.concat(
        [
            intervals.data,
            pd.DataFrame(
                {
                    "start_time": [pd.Timestamp(2)],
                    "end_time": [pd.Timestamp(8)],
                    "test1": 3,
                    "test2": 5,
                }
            ),
        ]
    )

    # no intersections
    with pytest.raises(ValueError):
        intervals.extract_interval(pd.Timestamp(13), pd.Timestamp(14))

    # intersects, not contained in any
    with pytest.raises(ValueError):
        intervals.extract_interval(pd.Timestamp(9), pd.Timestamp(12))

    # contained in multiple
    with pytest.raises(ValueError):
        intervals.extract_interval(pd.Timestamp(5), pd.Timestamp(7))

    assert (
        intervals.extract_interval(pd.Timestamp(4), pd.Timestamp(7))
        == pd.Series(
            {
                "start_time": pd.Timestamp(2),
                "end_time": pd.Timestamp(8),
                "test1": 3,
                "test2": 5,
            }
        )
    ).all()


def test_shift_by():
    intervals = Intervals.from_list(
        [
            (pd.Timestamp(0), pd.Timestamp(4), (3, 4)),
            (pd.Timestamp(5), pd.Timestamp(7), (4, 4)),
            (pd.Timestamp(7), pd.Timestamp(10), (3, 5)),
            (pd.Timestamp(11), pd.Timestamp(13), (3, 5)),
        ],
        column_names=["test1", "test2"],
    )

    def by(row):
        assert list(row.index) == ["test1", "test2"]
        if row["test1"] == 3:
            return pd.Timedelta(100)
        else:
            return pd.Timedelta(-5)

    new_intervals = intervals.shift_by(by)
    expected_new_intervals = Intervals.from_list(
        [
            (pd.Timestamp(100), pd.Timestamp(104), (3, 4)),
            (pd.Timestamp(0), pd.Timestamp(2), (4, 4)),
            (pd.Timestamp(107), pd.Timestamp(110), (3, 5)),
            (pd.Timestamp(111), pd.Timestamp(113), (3, 5)),
        ],
        column_names=["test1", "test2"],
    )
    assert compare_intervals(new_intervals, expected_new_intervals)


def test_limit_time():
    intervals = Intervals.from_list(
        [
            (pd.Timestamp(0), pd.Timestamp(4), (3, 4)),
            (pd.Timestamp(5), pd.Timestamp(7), (4, 4)),
            (pd.Timestamp(7), pd.Timestamp(10), (3, 5)),
            (pd.Timestamp(11), pd.Timestamp(13), (3, 5)),
        ],
        column_names=["test1", "test2"],
    )

    new_intervals = intervals.limit_time(pd.Timestamp(6), pd.Timestamp(12))
    expected_new_intervals = Intervals.from_list(
        [
            (pd.Timestamp(6), pd.Timestamp(7), (4, 4)),
            (pd.Timestamp(7), pd.Timestamp(10), (3, 5)),
            (pd.Timestamp(11), pd.Timestamp(12), (3, 5)),
        ],
        column_names=["test1", "test2"],
    )
    assert compare_intervals(new_intervals, expected_new_intervals)


def test_intersect_on_column():
    intervals = Intervals.from_list(
        [
            (pd.Timestamp(0), pd.Timestamp(4), (3, 4)),
            (pd.Timestamp(5), pd.Timestamp(7), (4, 4)),
            (pd.Timestamp(7), pd.Timestamp(10), (3, 5)),
            (pd.Timestamp(11), pd.Timestamp(13), (3, 5)),
        ],
        column_names=["test1", "test2"],
    )
    intervals2 = Intervals.from_list(
        [
            (pd.Timestamp(0), pd.Timestamp(3), (3, "a")),
            (pd.Timestamp(10), pd.Timestamp(11), (4, "b")),
            (pd.Timestamp(9), pd.Timestamp(15), (3, "c")),
        ],
        column_names=["other_test1", "other_test2"],
    )

    new_intervals = intervals.intersect_on_column(
        intervals2,
        self_col="test1",
        other_col="other_test1",
        self_cols_to_keep=["test2"],
        other_cols_to_keep=["other_test1", "other_test2"],
    )

    expected_new_intervals = Intervals.from_list(
        [
            (pd.Timestamp(0), pd.Timestamp(3), (4, 3, "a")),
            (pd.Timestamp(9), pd.Timestamp(10), (5, 3, "c")),
            (pd.Timestamp(11), pd.Timestamp(13), (5, 3, "c")),
        ],
        column_names=["test2", "other_test1", "other_test2"],
    )

    assert compare_intervals(new_intervals, expected_new_intervals)

    # Check that it can produce an empty intersection
    empty_intersection = intervals.intersect_on_column(
        Intervals.from_list(
            [(pd.Timestamp(100), pd.Timestamp(101), (3, 4))],
            column_names=["test1", "test2"],
        ),
        self_col="test1",
        other_col="test2",
        self_cols_to_keep=[],
        other_cols_to_keep=[],
    )

    assert compare_intervals(
        empty_intersection, Intervals.from_list([], column_names=[])
    )
