TITLES_DATASET = 'title.basics.tsv'
NAMES_DATASET = 'name.basics.tsv'
PRINCIPALS_DATASET = 'title.principals.tsv'
RATINGS_DATASET = 'title.ratings.tsv'
DATASET_PATHS = [('title', TITLES_DATASET),
                 ('name', NAMES_DATASET),
                 ('principals', PRINCIPALS_DATASET),
                 ('ratings', RATINGS_DATASET)]
DEFAULT_DATABASE_URI = "postgresql+psycopg2://postgres@127.0.0.1:5433/postgres"
CSV_EXTENSION = 'csv'
