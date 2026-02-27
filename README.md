# FastAPI + PostgreSQL Boilerplate

This is a simple boilerplate for a backend application using FastAPI and PostgreSQL.

## Setup

1.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Environment Variables**:
    Copy `.env.example` to `.env` and adjust the values if necessary.
    ```bash
    cp .env.example .env
    ```

3.  **Run Database & Cache**:
    Use Docker Compose to start the PostgreSQL database and Redis cache.
    ```bash
    docker-compose up -d
    ```
    *Note: If Docker is not available, the app will automatically fall back to an in-memory cache.*

4.  **Run Application**:
    Start the FastAPI server.
    ```bash
    uvicorn app.main:app --reload
    ```

## API Documentation

Once the application is running, you can access the interactive API documentation at:
-   Swagger UI: http://127.0.0.1:8000/docs
-   ReDoc: http://127.0.0.1:8000/redoc

## Project Structure

-   `app/main.py`: Application entry point and router configuration.
-   `app/database.py`: Database connection and session management.
-   `app/models.py`: SQLAlchemy database models.
-   `app/schemas.py`: Pydantic models for data validation.
-   `app/crud.py`: Database CRUD operations.
