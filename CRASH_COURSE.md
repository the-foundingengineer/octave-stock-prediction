# FastAPI + SQLAlchemy + PostgreSQL Crash Course

Welcome to your boilerplate! This document explains "how it works" and "why it works". It covers the libraries used, the architectural decisions, and the flow of data.

---

## 1. The Tech Stack

### **FastAPI** (The Web Framework)
-   **What it is**: A modern, fast (high-performance) web framework for building APIs with Python 3.6+ based on standard Python type hints.
-   **Why we use it**:
    -   **Speed**: It's one of the fastest Python frameworks available (comparable to NodeJS and Go).
    -   **Type Safety**: Heavily relies on Pydantic and Python types, reducing bugs.
    -   **Auto-Documentation**: Automatically generates interactive API docs (Swagger UI) at `/docs`.
-   **Key Concepts**:
    -   **Path Operations**: Functions decorated with `@app.get`, `@app.post`, etc.
    -   **Dependency Injection (`Depends`)**: A powerful system to inject dependencies (like database sessions) into your route handlers.

### **SQLAlchemy** (The ORM - Object Relational Mapper)
-   **What it is**: The standard SQL toolkit for Python. It allows you to interact with databases using Python classes and objects instead of raw SQL queries.
-   **Why we use it**: It abstracts the database system (PostgreSQL, SQLite, MySQL) so you can switch easily, and it handles the heavy lifting of converting SQL rows to Python objects.
-   **Key Concepts**:
    -   **Engine**: The starting point for any SQLAlchemy application. It handles the connection pool.
    -   **Session**: The handle to the database for a specific request. You add/update objects in a session and then `commit` them to save to the DB.
    -   **Model**: A Python class that represents a database table (defined in `models.py`).

### **Pydantic** (Data Validation)
-   **What it is**: Data validation and settings management using Python type hints.
-   **Why we use it**: To define the "shape" of data coming IN to the API and going OUT of the API.
-   **Key Concepts**:
    -   **BaseModel**: The class you inherit from to define a schema.
    -   **Validation**: If a client sends `"age": "hello"`, Pydantic will catch that it's not an integer and return a nice error automatically.

### **PostgreSQL** (The Database)
-   **What it is**: A powerful, open-source object-relational database system.
-   **Why we use it**: It's the industry standard for production-grade applications due to its reliability and feature set.

### **Docker & Docker Compose**
-   **What it is**: A tool to package code and dependencies into "containers".
-   **Why we use it**: To run the PostgreSQL database easily without installing it directly on your Windows machine. `docker-compose up` spins up the DB in seconds.

---

## 2. The Architecture (Logic Flow)

This boilerplate follows a standard "Service/Repository" pattern (simplified for FastAPI):

`Request` -> `Main (Router)` -> `Schema (Validation)` -> `CRUD (Logic)` -> `Model (DB)`

### The Directories & Files

#### 1. `app/database.py` (The Connection)
-   **Purpose**: Sets up the database connection.
-   **engine**: The persistent connection to Postgres.
-   **SessionLocal**: A factory to create a new database session for every request.
-   **Base**: The class that all our Models will inherit from.

#### 2. `app/models.py` (The Database Tables)
-   **Purpose**: Defines what the data looks like **in the database**.
-   **Logic**: Each class (e.g., `StockRecord`) inherits from `Base`.
-   **Columns**: Attributes like `Column(String)` become columns in your SQL table.
-   **Note**: These are strictly for the database. We don't usually send these directly to the user (we convert them to Schemas first).

#### 3. `app/schemas.py` (The API Contract / DTOs)
-   **Purpose**: Defines what the data looks like **in the API** (JSON).
-   **Logic**:
    -   `StockBase`: Fields common to reading and writing.
    -   `StockCreate`: Fields required when creating a new item (maybe passwds, inputs).
    -   `Stock`: Fields returned to the user (includes `id` which is DB-generated).
-   **'orm_mode = True'**: This magic config tells Pydantic "It's okay to read data from a SQLAlchemy class, not just a dictionary."

#### 4. `app/crud.py` (The Business Logic)
-   **Purpose**: Create, Read, Update, Delete operations.
-   **Logic**: This is where the magic happens.
    -   It takes a `db` Session.
    -   It takes Pydantic schemas as input (data from user).
    -   It creates SQLAlchemy Models (to save to DB).
    -   `db.add()`, `db.commit()`, `db.refresh()`: The standard dance to save data.

#### 5. `app/main.py` (The Entry Point)
-   **Purpose**: Ties everything together.
-   **`get_db`**: A dependency function. It opens a DB session, yields it to the route, and closes it *immediately* after the request is done. This prevents connection leaks.
-   **Routes**:
    -   `@app.post("/users/", response_model=schemas.User)`:
        -   Validation: FastAPI uses `user: schemas.UserCreate` to ensure the input is valid.
        -   Dependency: `db: Session = Depends(get_db)` gives us a DB session.
        -   Execution: Calls `crud.create_user(db, user)`.
        -   Response: Pydantic converts the returned DB object back into JSON using `response_model`.

---

## 3. How a Request Works (Step-by-Step)

Imagine a user sends a **POST** request to create a stock record:

1.  **Receive**: FastAPI receives the JSON payload.
2.  **Validate**: Pydantic (`schemas.StockCreate`) checks if the fields (`date`, `open`, etc.) are correct. If not, error 422.
3.  **Dependency**: FastAPI calls `get_db()` and creates a dedicated database session.
4.  **Route**: The function `create_stock` in `main.py` is called.
5.  **Logic**: `main.py` calls `crud.create_stock`.
6.  **DB Operation**:
    -   `crud.py` turns the Pydantic data into a SQLAlchemy Model (`models.StockRecord`).
    -   It adds it to the session and commits transaction to PostgreSQL.
7.  **Return**: The database returns the saved object (now with an `id`).
8.  **Serialize**: FastAPI uses `schemas.Stock` (the `response_model`) to convert that SQLAlchemy object back to JSON.
9.  **Close**: `get_db` cleans up and closes the session.

---

## 4. Key Commands

**Start the DB**:
```bash
docker-compose up -d
```

**Run the App**:
```bash
# --reload makes it auto-restart when you save files
uvicorn app.main:app --reload
```

**Import Data**:
```bash
python import_stocks.py
```
