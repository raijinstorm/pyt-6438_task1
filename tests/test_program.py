import json
import pytest
from pydantic import ValidationError

from query_db import Room, Student, extract_from_json


def test_room_class_valid_data():
    r = Room(id=1, name="Name")
    assert r.id == 1
    assert r.name == "Name"


def test_student_class_invalid_data():
    with pytest.raises(ValidationError):
        Student(id=2, birthday="2000-01-01", name="Alice", room=1, sex="X")


def test_extract_from_json(tmp_path):
    """extract_from_json should return correct row lists."""
    rooms = [{"id": 1, "name": "Math"}, {"id": 2, "name": "History"}]
    students = [
        {"birthday": "2001-02-03", "id": 10, "name": "Bob", "room": 1, "sex": "M"}
    ]
    rooms_file = tmp_path / "rooms.json"
    students_file = tmp_path / "students.json"
    with open(str(rooms_file), "w") as f:
        json.dump(rooms, f)

    with open(str(students_file), "w") as f:
        json.dump(students, f)

    rows_rooms, rows_students = extract_from_json(str(rooms_file), str(students_file))
    assert rows_rooms == [[1, "Math"], [2, "History"]]
    assert rows_students == [["2001-02-03", 10, "Bob", 1, "M"]]
