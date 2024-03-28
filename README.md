# Danelfin_demo

## Description
Project for Managing and Adding Customers. This project has an easy-to-use storage system. It checks data carefully, records details, and tests everything well. Everything is neatly arranged and easy to understand. It also focuses on managing errors well. Plus, the project uses Docker to keep everything running smoothly in its own container

## Development
To prevent dependency conflicts, installing a virtual environment via the `venv` library is advisable. Being a Python package, installation in development mode is required for console execution. Use the command `pip install -e .` to install the package in development mode, allowing it to be executed from the console using `python danelfin_demo/main.py`.

## Rubric
- Utilize `TinyDB` to create a database for client storage.
- The `pydantic` package has been used for data validation.
- The `loguru` logging package has been used for log recording.
- The `pytest` package has been used for the creation of tests.
- A Docker image of the project should be generated using a `Dockerfile` where the entrypoint is the project's main script.
- For `pyarrow` dump, it is recommended to use a schema to avoid problems with data types.
- The project has a clear, modular structure and is well documented.


## Methods specifications
- Add customer
    ```
    params:
        --name <string>
        --email <string> check for valid email
        --age <int>
        --country <str>

    output:
        {
            "id": <int>,
            "name": <string>,
            "email": <string>,
            "age": <int>,
            "country": <str>
        }
    
    example:
        python danelfin_demo/main.py add_customer --name Peter --email peter@test.com --age 32 --country ES

    ```
- Get customer
    ```
    params:
        --id <int>

    output:
        {
            "id": <int>,
            "name": <string>,
            "email": <string>,
            "age": <int>,
            "country": <str>
        }
    
    example:
        python danelfin_demo/main.py get_client --id 1
    ```

- Dump clients
    ```
        output:
            .parquet file with all the customers, partitioned by country
        
        example:
            python danelfin_demo/main.py dump
    ```
