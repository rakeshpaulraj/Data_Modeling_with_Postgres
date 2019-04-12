from sqlalchemy import MetaData
from sqlalchemy_schemadisplay import create_schema_graph

connection = "postgres://student:student@localhost/sparkifydb"
graph = create_schema_graph(metadata=MetaData(connection), 
                            show_datatypes=True, # show datatypes
                            show_indexes=True, # show index (in ourcase unique)
                            rankdir='LR', # left to right alignment
                            concentrate=False)
graph.write_png('database_schema_diagram.png')