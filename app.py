import os
import pickle
import time

from flask import Flask, render_template, request, url_for
from flask_autoindex import AutoIndex
from pyspark.sql import SparkSession
from werkzeug.utils import redirect, secure_filename

from form_clases.forms import database_form, run_form
from jobs.common_functions import query_executor
from jobs.report_builder import put_raw_data

basedir = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__)
ppath = "download_reports"

files_index = AutoIndex(app, browse_root=ppath, add_url_rules=False)


@app.route("/download_reports")
@app.route("/files/<path:path>")
def autoindex(path="."):
    return files_index.render_autoindex(path)


app.config["SECRET_KEY"] = "Thisisasecret!"
spark = (
    SparkSession.builder.appName("simple-sf-test")
    .config(
        "spark.jars.packages",
        "net.snowflake:spark-snowflake_2.11:2.7.0-spark_2.4,"
        "org.postgresql:postgresql:42.2.9.jre7",
    )
    .getOrCreate()
)


def main(
    spark,
    db_type,
    db_connect,
    file_name,
    run_name,
    query_id_list=None,
    total_limit=10,
    run_for=60,
    table_name="",
):

    query_res_list = []
    start_time = time.monotonic()
    while True:
        query_list = query_executor(
            spark, db_type, db_connect, file_name, query_id_list, total_limit
        )
        query_res_list.extend(query_list)
        end_time = time.monotonic()
        time.sleep(2)
        if end_time - start_time > run_for:
            break
    file_name = put_raw_data(db_type, db_connect, run_name, query_res_list, table_name)
    return file_name


@app.route("/", methods=["GET", "POST"])
def index():
    return render_template("index.html")


@app.route("/setup", methods=["GET", "POST"])
def form():
    form = database_form()

    if form.validate_on_submit():
        content = request.form.to_dict()
        print(form.file.data)
        filename = secure_filename(form.file.data.filename)
        content["filename"] = filename
        form.file.data.save("uploads/" + filename)
        env_name = content["env_name"]
        with open(f"/tmp/{env_name}", "wb") as fp:
            pickle.dump(content, fp)
        print(content)
        return render_template("run_test.html", form=run_form())
    return render_template("form.html", form=form)


@app.route("/run", methods=["GET", "POST"])
def run_test():
    form = run_form()
    if form.validate_on_submit():
        content = request.form.to_dict()
        env_name = content["env_name"]
        with open(f"/tmp/{env_name}", "rb") as fp:
            p_dict = pickle.load(fp)
        db_type = p_dict["db_type"]
        table_name = p_dict["table_name"]
        file_name = f"uploads/{p_dict['filename']}"
        run_name = content["run_name"]
        query_str = content["query_id"]
        query_str = query_str.replace(" ", "")
        query_id_list = query_str.split(",")
        total_limit = int(content["total_runs"])
        if total_limit < len(query_id_list):
            total_limit = len(query_id_list)
        run_for = int(content["total_time"])
        main(
            spark,
            db_type,
            p_dict,
            file_name,
            run_name,
            query_id_list=query_id_list,
            total_limit=total_limit,
            run_for=run_for,
            table_name=table_name,
        )
        return redirect(url_for("autoindex"))
    return render_template("run_test.html", form=form)


if __name__ == "__main__":
    app.run(debug=True)
