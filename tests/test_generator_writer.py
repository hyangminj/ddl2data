from ddl2data.generator.base import generate_all
from ddl2data.generator.dist import parse_dist_arg
from ddl2data.parser.ddl import parse_ddl_text
from ddl2data.parser.graph import generation_order
from ddl2data.writer.postgres import render_insert_sql


DDL = """
CREATE TABLE users (
  id INT PRIMARY KEY,
  age INT,
  tier VARCHAR(10)
);

CREATE TABLE events (
  id INT PRIMARY KEY,
  user_id INT,
  score INT,
  FOREIGN KEY (user_id) REFERENCES users(id)
);
"""


def test_generate_and_postgres_writer_smoke():
    tables = parse_ddl_text(DDL)
    order = generation_order(tables)
    dist = dict(
        [
            parse_dist_arg("age:normal,mean=30,std=5"),
            parse_dist_arg("tier:weighted,A=60%,B=40%"),
            parse_dist_arg("score:poisson,lambda=3"),
        ]
    )

    data = generate_all(tables, order, rows=5, dist_overrides=dist)
    assert set(data.keys()) == {"users", "events"}
    assert len(data["users"]) == 5
    assert len(data["events"]) == 5
    assert all(r["user_id"] is not None for r in data["events"])

    sql = render_insert_sql("users", data["users"])
    assert "INSERT INTO \"users\"" in sql
    assert sql.count("INSERT INTO") == 5


def test_table_qualified_dist_override_priority():
    ddl = """
    CREATE TABLE users (
      id INT PRIMARY KEY,
      score INT
    );

    CREATE TABLE events (
      id INT PRIMARY KEY,
      user_id INT,
      score INT,
      FOREIGN KEY (user_id) REFERENCES users(id)
    );
    """
    tables = parse_ddl_text(ddl)
    order = generation_order(tables)
    dist = dict(
        [
            parse_dist_arg("score:weighted,global=100%"),
            parse_dist_arg("users.score:weighted,U=100%"),
        ]
    )
    data = generate_all(tables, order, rows=5, dist_overrides=dist)

    assert all(r["score"] == "U" for r in data["users"])
    assert all(r["score"] == "global" for r in data["events"])
