# pglineage

## Overview
pglineage is a tool to create data flow diagrams (column-level and table-level) in PostgreSQL by analyzing SQL.

## Installation
To use pglineage, follow these steps:

1. Download:
    ~~~bash
    git clone https://github.com/gtk7032/pglineage.git
    ~~~  

1. Build the Docker image:
    ~~~bash
    docker-compose build --build-arg UID="`id -u`" --build-arg GID="`id -g`"
    ~~~
    The UID and GID arguments will be assigned to the user used within the container.

1. Launch the container:

    ~~~bash
    docker-compose up -d
    ~~~

## Usage

1. See the [sample](src/sample.py), which targets the SQL contained in all files within the resource folder:

    ~~~bash
    docker exec -it pglineage python src/sample.py
    ~~~ 

1. Verify that a data flow diagram has been generated in the output folder.

    A file name with "tlv" indicates the table level, while "clv" indicates the column level.

## Notice
Some grammars, such as UPSERT, are not supported at this time.

## License
[MIT](https://choosealicense.com/licenses/mit/)

## Author
[gtk7032](https://github.com/gtk7032)




