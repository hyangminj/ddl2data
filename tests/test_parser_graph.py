from ddl2data.parser.ddl import parse_ddl_text
from ddl2data.parser.graph import generation_order


DDL = """
CREATE TABLE users (
  id INT PRIMARY KEY,
  email VARCHAR(120) NOT NULL,
  tier VARCHAR(10)
);

CREATE TABLE orders (
  id INT PRIMARY KEY,
  user_id INT,
  amount NUMERIC,
  CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)
);
"""


def test_parse_and_graph_order():
    tables = parse_ddl_text(DDL)
    assert len(tables) == 2
    by_name = {t.name: t for t in tables}
    assert "users" in by_name
    assert "orders" in by_name
    assert any(c.name == "email" and c.max_length == 120 for c in by_name["users"].columns)
    assert by_name["orders"].foreign_keys[0].ref_table == "users"

    order = generation_order(tables)
    assert order.index("users") < order.index("orders")
