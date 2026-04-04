from flask import Flask, request, g, jsonify, url_for, Response
from flask.typing import ResponseReturnValue
from farmfs import getvol, cwd
from farmfs.fs import Path
from docopt import docopt
from typing import Optional

from farmfs.volume import FarmFSVolume

API_USAGE = """
    Farmfs API endpoint.

    Usage:
      farmapi [options]

    Options:
      --host=<host>  Host interface to bind to [default: 127.0.0.1].
      --port=<port>  Port to run the Flask app on [default: 5000].
      --root=<root>  Where the farmfs depot is located.
      -h --help      Show this help message.
    """

def get_app(args: dict[str, str]) -> Flask:
    app = Flask("farmfs")

    @app.before_request
    def get_volume() -> None:
        g.vol = getvol(Path(args.get("<root>", cwd)))

    @app.route("/bs", methods=["POST"])
    def blob_create() -> ResponseReturnValue:
        """
        Create a new blob in the blobstore.
        blob is a required argument, which is the md5 checksum of the blob content.
        """
        headers = {}
        vol: FarmFSVolume = g.vol
        blob = request.args["blob"]
        try:
            upload_fd = request.stream
            # HTTP doesn't give us retry capability on upload_fd
            with vol.bs.session() as sess:
                duplicate = sess.import_via_fd(lambda: upload_fd, blob)
            if duplicate:
                status = 200
            else:
                status = 201
            headers["Location"] = url_for("blob_get_head", blob=blob, _external=True)
            return jsonify({"duplicate": duplicate, "blob": blob}), status, headers
        except Exception as e:
            return jsonify({"error": str(e)}), 500, headers

    def blob_read(blob: str) -> ResponseReturnValue:
        """
        Read a blob. <blob> is the md5 checksum of the content.
        Returns 404 if not found.
        """
        vol: FarmFSVolume = g.vol
        try:
            response = Response(
                vol.bs.blob_chunks(blob, 4096), content_type="application/octet-stream"
            )
        except FileNotFoundError:
            return "", 404, {}
        return response, 200, {}

    # TODO: Update (not needed for immutable store? Maybe for fixing corruptions?)

    def blob_delete(blob) -> ResponseReturnValue:
        """
        Delete a blob from the blobstore.
        """
        vol = g.vol
        try:
            vol.bs.delete_blob(blob)
        except FileNotFoundError:
            return "", 204
        else:
            return "", 204

    @app.route("/bs", methods=["GET"])
    def blob_list() -> ResponseReturnValue:
        """
        List blobs in the blobstore, with optional cursor-based paging.

        Query params:
          start-after=<blob>  Return blobs strictly after this checksum (exclusive).
          max_items=<n>       Maximum number of blobs to return per page.

        Response JSON:
          {"blobs": [...], "next": "<blob>" | null}
          "next" is the last blob on this page; pass it as "start-after" for
          the next page. null when this is the last page.
        """
        vol = g.vol
        bs = vol.bs
        start_after = request.args.get("start-after", None)
        max_items_str = request.args.get("max_items", None)
        max_items = int(max_items_str) if max_items_str is not None else None

        page = list(bs.blobs(start_after=start_after, max_items=max_items))
        next_cursor: Optional[str] = page[-1] if (max_items is not None and len(page) == max_items) else None

        return jsonify({"blobs": page, "next": next_cursor}), 200

    def blob_exists(blob) -> ResponseReturnValue:
        """
        Return if a blob is in the blobstore.
        """
        vol = g.vol
        exists = vol.bs.exists(blob)
        if exists:
            return "", 200
        else:
            return "", 404

    @app.route("/bs/<blob>", methods=["HEAD", "GET", "DELETE"])
    def blob_get_head(blob):
        """Router on blob operations to different verbs"""
        if request.method == "HEAD":
            return blob_exists(blob)
        elif request.method == "GET":
            return blob_read(blob)
        elif request.method == "DELETE":
            return blob_delete(blob)
        else:
            return "", 405  # Method Not Allowed

    @app.route("/bs/<blob>/checksum", methods=["GET"])
    def blob_get_checksum(blob) -> ResponseReturnValue:
        vol = g.vol
        csum = {"csum": vol.bs.blob_checksum(blob)}
        return csum

    return app


def api_main() -> None:
    args = docopt(API_USAGE)
    app = get_app(args)
    app.run(debug=True, host=args["--host"], port=int(args["--port"]))
