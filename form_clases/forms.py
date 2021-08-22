from flask_wtf import FlaskForm
from wtforms import FileField, IntegerField, PasswordField, SelectField, StringField
from wtforms.validators import InputRequired


class database_form(FlaskForm):

    env_name = StringField("Enviornment Name", validators=[InputRequired()])
    db_type = SelectField(
        "Database Type", choices=[("Snowflake"), ("Oracle"), ("Postgres"), ("MySql"), ("Hive")]
    )
    host = StringField(
        "Host Address",
        validators=[InputRequired()],
    )
    port = StringField("port", validators=[InputRequired()])
    username = StringField("username", validators=[InputRequired()])
    password = PasswordField("Password")
    dbname = StringField("Database Name")
    table_name = StringField("Table Name")
    sid = StringField("Oracle Database SID")
    role = StringField("role")
    warehouse = StringField("warehouse")
    schema = StringField("schema")
    database = StringField("database")
    file = FileField("file")


class run_form(FlaskForm):
    env_name = StringField("Env Name", validators=[InputRequired()])
    run_name = StringField("Run Name", validators=[InputRequired()])
    query_id = StringField("Query Id", validators=[InputRequired()])
    stagger_for = StringField("Stagger For", validators=[InputRequired()])
    total_runs = IntegerField("Total Number of Runs", validators=[InputRequired()])
    total_time = IntegerField("Total Time", validators=[InputRequired()])
