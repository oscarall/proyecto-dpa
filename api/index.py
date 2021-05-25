
from flask import Flask, request, abort, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import cast, DATE
from flask_restplus import Api, Resource, fields
from src.utils.general import get_db_conn_sql_alchemy
from src.utils.constants import CREDENTIALS_FILE
from werkzeug.exceptions import NotFound


#Connect to DB
db_conn_str = get_db_conn_sql_alchemy(CREDENTIALS_FILE)

#create flask app
app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = db_conn_str
api = Api(app)

@api.errorhandler(NotFound)
def handle_no_result_exception(error):
    return {'message': "Inspection not found"}, 404


db = SQLAlchemy(app)
ns = api.namespace('inspections', description='RESTful API')

inspection = api.model('Inspection', {
    'inspection_id': fields.Integer(),
    'name': fields.String(),
    'risk': fields.String(),
    'address': fields.String(),
    'zip_code': fields.Integer(),
    'inspection_date': fields.String(),
    'predicted_date': fields.String(),
    'predicted': fields.String()
})

#crear tabla de deploy
class Scores(db.Model):
    __table_args__ = {'schema' : 'api'}
    __tablename__ = 'scores'

    id = db.Column(db.Integer, primary_key = True)
    inspection_id  = db.Column(db.Integer)
    name = db.Column(db.String(255))
    risk = db.Column(db.String(255))
    address = db.Column(db.String(512))
    zip_code = db.Column(db.Integer) 
    inspection_date = db.Column(db.DateTime)
    predicted_date = db.Column(db.DateTime)
    predicted = db.Column(db.String(4))
    
    def as_dict(self):
        return {
            'inspection_id': self.inspection_id,
            'name': self.name,
            'risk': self.risk,
            'address': self.address,
            'zip_code': self.zip_code,
            'inspection_date': self.inspection_date.strftime("%m/%d/%Y %H:%M:%S"),
            'predicted_date': self.predicted_date.strftime("%m/%d/%Y %H:%M:%S"),
            'predicted': self.predicted
        }


#endpoints
@ns.route('/<int:inspection_id>')
@ns.param('inspection_id', 'The inspection identifier')
class ShowPredID(Resource):
    @ns.response(200, "Ok", model=inspection)
    def get(self, inspection_id):
        prediction = Scores.query.filter_by(inspection_id=inspection_id).first()

        if not prediction:
            raise NotFound

        return prediction.as_dict()


@ns.route('/')
@ns.doc(params={'date': {'description': 'Date to search in format YYYY-mm-dd', 'in': 'query', 'type': 'string'}})
class ShowFechaPrediccion(Resource):
    @ns.response(200, "Ok", model=[inspection])
    def get(self):
        date = request.args.get('date', None)
        query = Scores.query
        
        if date:
            query = query.filter(cast(Scores.predicted_date, DATE) == date)
        
        results = query.order_by(Scores.predicted_date.desc()).all()
        
        return list(map(lambda inspection: inspection.as_dict(), results))


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
