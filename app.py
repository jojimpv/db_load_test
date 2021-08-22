import os
import pickle
import time

from flask import Flask, render_template, request, url_for
from flask_autoindex import AutoIndex
from werkzeug.utils import redirect, secure_filename
from pathlib import Path

from form_clases.forms import database_form, run_form
from jobs.common_functions import query_executor
from jobs.report_builder import put_raw_data
from project_settings import get_root_path
from st_utils.logger import get_logger

logger = get_logger(__name__)

basedir = os.path.abspath(os.path.dirname(__file__))
root_path = get_root_path()

app = Flask(__name__)
if not Path(f"{root_path}/download_reports").is_dir():
    os.mkdir(f"{root_path}/download_reports")

ppath = f"{root_path}/download_reports"
files_index = AutoIndex(app, browse_root=ppath, add_url_rules=False)


@app.route("/download_reports")
@app.route("/files/<path:path>")
def autoindex(path="."):
    return files_index.render_autoindex(path)


app.config["SECRET_KEY"] = "Thisisasecret!"


def main(
    db_type,
    db_connect,
    file_name,
    stagger_for,
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
            db_type, db_connect, file_name, stagger_for, query_id_list, total_limit
        )
        query_res_list.extend(query_list)
        end_time = time.monotonic()
        time.sleep(2)
        if end_time - start_time > run_for:
            break
    file_name = put_raw_data(db_type, db_connect, run_name, query_res_list, table_name, root_path)
    return file_name


@app.route("/", methods=["GET", "POST"])
def index():
    return render_template("index.html")


@app.route("/setup", methods=["GET", "POST"])
def form():
    form = database_form()


    if not Path(f"{root_path}/uploads").is_dir():
        os.mkdir(f"{root_path}/uploads")

    if form.validate_on_submit():
        content = request.form.to_dict()
        filename = secure_filename(form.file.data.filename)
        content["filename"] = filename
        form.file.data.save(f"{root_path}/uploads/" + filename)
        env_name = content["env_name"]
        with open(f"/tmp/{env_name}", "wb") as fp:
            pickle.dump(content, fp)
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
        file_name = f"{root_path}/uploads/{p_dict['filename']}"
        stagger_for = content["stagger_for"]
        run_name = content["run_name"]
        query_str = content["query_id"]
        query_str = query_str.replace(" ", "")
        query_id_list = query_str.split(",")
        total_limit = int(content["total_runs"])
        if total_limit < len(query_id_list):
            total_limit = len(query_id_list)
        run_for = int(content["total_time"])
        try:
            main(
                db_type,
                p_dict,
                file_name,
                stagger_for,
                run_name,
                query_id_list=query_id_list,
                total_limit=total_limit,
                run_for=run_for,
                table_name=table_name,
            )
            return redirect(url_for("autoindex"))
        except Exception as error:
            logger.error(f"error executing run , error: {error}")
            return render_template("bad_response.html", form=form, output=error)


if __name__ == "__main__":
    app.run(debug=True)
