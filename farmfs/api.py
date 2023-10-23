from flask import Flask, request, g, jsonify
from farmfs import getvol, cwd
from farmfs.fs import Path
from docopt import docopt

API_USAGE = """
    Farmfs API endpoint.

    Usage:
      farmapi [options]

    Options:
      --port=<port>  Port to run the Flask app on [default: 5000].
      --root=<root>  Where the farmfs depot is located.
      -h --help      Show this help message.
    """

def get_app(args):
    app = Flask('farmfs')

    @app.before_request
    def get_volume():
        g.vol = getvol(Path(args.get('<root>', cwd)))

    @app.route('/bs', methods=['POST'])
    def blob_create():
        """
        Create a new blob in the blobstore.
        blob is a required argument, which is the md5 checksum of the blob content.
        """
        vol = g.vol
        blob = request.args['blob']
        try:
            upload_fd = request.stream
            # HTTP doesn't give us retry capability on upload_fd
            duplicate = vol.bs.import_via_fd(lambda: upload_fd, blob, tries=1)
            return jsonify({"duplicate": duplicate,
                            "blob": blob}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    def blob_read(blob):
        """
        Read a blob. <blob> is the md5 checksum of the content.
        Returns 404 if not found.
        """
        raise NotImplementedError("Have not implemented read.")

    # TODO: Update (not needed for immutable store? Maybe for fixing corruptions?)

    def blob_delete(blob):
        """
        Delete a blob from the blobstore.
        """
        vol = g.vol
        vol.bs.delete_blob(blob)
        return '', 204

    @app.route('/bs', methods=['GET'])
    def blob_list():
        """
        List all the blobs in the blobstore.
        """
        vol = g.vol
        bs = vol.bs
        return jsonify(list(bs.blobs())), 200

    def blob_exists(blob):
        """
        Return if a blob is in the blobstore.
        """
        vol = g.vol
        exists = vol.bs.exists(blob)
        if exists:
            return '', 200
        else:
            return '', 404

    @app.route('/bs/<blob>', methods=['HEAD', 'GET', 'DELETE'])
    def blob_get_head(blob):
        """Router on blob operations to different verbs"""
        if request.method == 'HEAD':
            return blob_exists(blob)
        elif request.method == 'GET':
            return blob_read(blob)
        elif request.method == 'DELETE':
            return blob_delete(blob)

    return app

def api_main():
    args = docopt(API_USAGE)
    app = get_app(args)
    app.run(debug=True, port=int(args['--port']))
