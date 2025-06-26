.PHONY: build up down logs shell ingest preprocess nlp index all clean

build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

shell:
	docker-compose exec app bash

ingest:
	docker-compose exec app python data_loader.py

preprocess:
	docker-compose exec app python preprocessing.py

nlp:
	docker-compose exec app python nlp_pipeline.py

index:
	docker-compose exec app python es_ingest.py

all: ingest preprocess nlp index

clean:
	docker-compose down --volumes --rmi all
