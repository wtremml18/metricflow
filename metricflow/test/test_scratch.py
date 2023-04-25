import logging
import textwrap
from typing import Any, Dict

import jinja2

from metricflow.test.table_snapshot.table_snapshots import TABLE_SNAPSHOT_REPOSITORY

logger = logging.getLogger(__name__)


def test_scratch() -> None:
    cote_divoire = "cote d'ivoire"
    data = [
        ("l3141592", "us", 2, True, "u0004114", "2020-01-01", "2020-01-02"),
        ("l2718281", cote_divoire, 4, True, "u0005432", "2020-01-01", "2020-01-02"),
        ("l3141592", "us", 3, True, "u0004114", "2020-01-02", None),
        # This cote_divoire property changed hands to a person from Maryland who considers it not lux
        ("l2718281", cote_divoire, 4, False, "u0003154", "2020-01-02", None),
        ("l5948301", "us", 5, True, "u0004114", "2020-01-02", None),
        ("l9658588-incomplete", None, None, None, "u1004114", "2020-01-01", "2020-01-02"),
        ("l9658588-incomplete", "us", None, None, "u1004114", "2020-01-02", None),
        ("l8912456-incomplete", None, None, None, "u1004114", "2020-01-02", None),
        ("l7891283-incomplete", "ca", None, False, "u1004114", "2020-01-02", None),
    ]

    new_data = []
    for row in data:
        new_data.append([transform_column(x) for x in row])

    col_num_to_max_length = {}
    for row in new_data:
        for col_num, col_value in enumerate(row):
            col_num_to_max_length[col_num] = max(col_num_to_max_length.get(col_num) or 0, len(col_value))

    buffer = ""

    columns_order_dict = {
        0: 5,
        1: 6,
        2: 0,
        3: 1,
        4: 2,
        5: 3,
        6: 4,
        7: 5,
    }

    sorted_data = sorted(new_data, key=lambda x: x[columns_order_dict[0]])
    for row in sorted_data:
        buffer += "    - ["
        first = True
        # for col_num, col_value in enumerate(row):
        for i, _ in enumerate(row):
            col_num = columns_order_dict[i]
            col_value = row[col_num]
            if first:
                first = False
            else:
                buffer += ", "

            # max_col_length = col_num_to_max_length[col_num]
            buffer += col_value
            # buffer += " " * (max_col_length - len(col_value))

        buffer += "]\n"

    logger.error(f"Data is:\n{buffer}")


def transform_column(col):
    if col is None:
        return "null"
    return f'"{col}"'


def test_scratch2() -> None:
    for table_snapshot in TABLE_SNAPSHOT_REPOSITORY.table_snapshots:
        logger.info(f"Table snapshot is: {table_snapshot.name}")


def render_column_type_entry(column_name: str, example_value: Any) -> str:
    column_type = "STRING"
    if isinstance(example_value, str):
        if example_value.startswith("20"):
            column_type = "TIME"
    elif isinstance(example_value, bool):
        column_type = "BOOLEAN"
    elif isinstance(example_value, float):
        column_type = "FLOAT"

    return jinja2.Template(
        textwrap.dedent(
            """\
            - name: {{ column_name }}
              type: {{ column_type }}
            """
        )
    ).render(
        column_name=column_name,
        column_type=column_type,
    )


def test_yaml() -> None:
    """
    create_table(
        sql_client=sql_client,
        sql_table=SqlTable(schema_name=schema, table_name="dim_companies"),
        df=make_df(
            sql_client=sql_client,
            columns=["company_id", "user_id", "company_name"],
            data=[
                ("cpid_0", "u0003154", "MD Vacation Rentals LLC"),
            ],
        ),
    )
    """
    DEFAULT_DS = "ds"
    cote_divoire = "cote d'ivoire"

    name = "dim_companies"
    columns = ["company_id", "user_id", "company_name"]
    data = [
        ("cpid_0", "u0003154", "MD Vacation Rentals LLC"),
    ]
    first_columns = ["company_id", "company_name", "user_id"]

    column_name_to_index: Dict[str, int] = {}
    for i, column_name in enumerate(columns):
        column_name_to_index[column_name] = i

    column_definition_section_lines = []
    for column_name in first_columns:
        column_definition_section_lines.append(
            render_column_type_entry(
                column_name=column_name, example_value=data[0][column_name_to_index[column_name]]
            )
        )

    for column_name in columns:
        if column_name in first_columns:
            continue
        column_definition_section_lines.append(
            render_column_type_entry(
                column_name=column_name, example_value=data[0][column_name_to_index[column_name]]
            )
        )

    column_definition_section = "\n".join(column_definition_section_lines)
    new_data = []
    for row in data:
        new_data.append([transform_column(x) for x in row])

    col_num_to_max_length = {}
    for row in new_data:
        for col_num, col_value in enumerate(row):
            col_num_to_max_length[col_num] = max(col_num_to_max_length.get(col_num) or 0, len(col_value))

    row_section = ""

    columns_order_dict = {}
    next_index = 0

    for column_name in first_columns:
        columns_order_dict[next_index] = column_name_to_index[column_name]
        next_index += 1

    for column_name in columns:
        if column_name in first_columns:
            continue
        columns_order_dict[next_index] = column_name_to_index[column_name]
        next_index += 1

    sorted_data = sorted(new_data, key=lambda x: x[columns_order_dict[0]])
    for row in sorted_data:
        row_section += "- ["
        first = True
        # for col_num, col_value in enumerate(row):
        for i, _ in enumerate(row):
            col_num = columns_order_dict[i]
            col_value = row[col_num]
            if first:
                first = False
            else:
                row_section += ", "

            # max_col_length = col_num_to_max_length[col_num]
            row_section += col_value
            # buffer += " " * (max_col_length - len(col_value))

        row_section += "]\n"

    rendered_yaml = jinja2.Template(
        textwrap.dedent(
            """\
            table_snapshot:
              name: {{ table_name }}
              column_definitions:
                {{ column_definition_section | indent(4) }}
              rows:
                {{ row_section | indent(4) }}
            """
        )
    ).render(
        table_name=name,
        column_definition_section=column_definition_section,
        row_section=row_section,
    )
    logger.error(f"YAML is:\n{rendered_yaml}")