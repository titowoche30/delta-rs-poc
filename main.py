import pandas as pd
from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import duckdb
pd.set_option('display.max_columns', None)


def get_data(s3_path) -> pd.DataFrame:
    return pd.read_csv(s3_path, sep=';')


def write_delta_table(df: pd.DataFrame, s3_path, s3_options):
    # s3_options = {"AWS_ACCESS_KEY_ID": "THE_AWS_ACCESS_KEY_ID",
    #                    "AWS_SECRET_ACCESS_KEY": "THE_AWS_SECRET_ACCESS_KEY"}

    write_deltalake(s3_path, df, mode='overwrite', storage_options=s3_options)


def read_delta_table(s3_path, s3_options):
    return DeltaTable(s3_path, storage_options=s3_options)


def get_pandas_grouped_data(delta_table):
    df = delta_table.to_pandas()
    return df.groupby(['Sex', 'Pclass'])['Survived'].sum().to_frame()


def get_duck_grouped_data(delta_table):
    arrow_dataset = delta_table.to_pyarrow_dataset()

    con = duckdb.connect(":memory:")
    duck_table = duckdb.from_arrow(arrow_dataset, con)

    query = """
        Select Sex, Pclass, sum(Survived)
        from duck_table
        group by Sex, Pclass
        order by Sex, Pclass
    """
    result = duckdb.sql(query, connection=con)

    result.show()
    con.close()


if __name__ == '__main__':
    # data_s3_path = 's3://cwoche-data-tests/datasets/titanic.csv'
    # titanic_data: pd.DataFrame = get_data(data_s3_path)

    delta_table_s3_path = 's3://cwoche-data-tests/datasets/delta_tables/titanic'
    s3_options = {
        "AWS_PROFILE": "default",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"}
    # write_delta_table(titanic_data, delta_table_s3_path)

    delta_table = read_delta_table(delta_table_s3_path, s3_options)
    # print(delta_table.metadata())
    # print(delta_table.schema())
    # print(delta_table.get_add_actions(flatten=True).to_pandas())

    pd_grouped_data = get_pandas_grouped_data(delta_table)
    print(pd_grouped_data.head())

    get_duck_grouped_data(delta_table)

