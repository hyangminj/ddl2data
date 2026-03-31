CREATE TABLE users (
 id INT PRIMARY KEY,
 name VARCHAR(30) NOT NULL
);
CREATE TABLE orders (
 id INT PRIMARY KEY,
 user_id INT,
 FOREIGN KEY (user_id) REFERENCES users(id)
);
