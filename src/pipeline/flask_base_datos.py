from flask import Flask
from flask import jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_restplus import Api, Resource, fields
from src.utils.general import get_db_conn_sql_alchemy

#Connect to DB
db_conn_str = get_db_conn_sql_alchemy('../../conf/local/credentials.yml')

#create flask app
app = Flask(__name__)
app.config('SQLALCHEMY_DATABASE_URI') = db_conn_str
api = Api(app)

db = SQLAlchemy(app)

#crear tabla de deploy
class Match(db.Model):
    __table_args__ = {'schema' : 'deploy'}
    __tablename__ = 'mockup_match_api'

    inspection_id = db.Column(db.Integer, primary_key = True)
    score = db.Column(db.Integer)
    fecha_prediccion = db.Column(db.DateTime)

    def __repr__(self):
        return(u'<{self, class, name}: {self, id}>'.format(self=self))

#model id
model = api.model("id_match_table", {
    'inspection_id': fields.Integer,
    'score': fields.Integer,
})

model_list = api.model("id_match_output", {
    'fecha_prediccion' : fields.DateTime,
    'total_prediccion' : fields.Nested(model)
})

#endpoings

@api.route('/')
class HelloWorld(Resource):
    def get(self):
        return {'Hello' : 'Equipo 2!'}

@api.route('/match score/<int:inspection_id>')
class ShowScoreID(Resource):
    def get(self, inspection_id):
        match = Match.query.filter_by(inspection_id=inspection_id).order_by(Match.inspection_id.desc()).all
        score = []
        for element in match:
            score.append({'score': element.score,
                          'fecha_prediccion': element.fecha_prediccion
                          })
        return {'inspection_id': inspection_id, 'score' : score}

@api.route('/match inspection_id/<DateTime:fecha_prediccion>')
class ShowFechaPrediccion(Resource):
    def get(self, fecha_prediccion):
        match = Match.query.filter_by(fecha_prediccion=fecha_prediccion).order_by(Match.fecha_prediccion.desc()).all
        inspection_id= []
        for element in match:
            inspection_id.append({'inspection_id': element.inspection_id,
                          'score': element.score,
                          })
        return {'fecha_prediccion': fecha_prediccion, 'inspection_id': inspection_id}

if __name__ == '__main__':
    app.run(debug=True)
