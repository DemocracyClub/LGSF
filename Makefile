.DEFAULT_GOAL := help

.PHONY: all
all: clean lambda-layers/DependenciesLayer/requirements.txt

.PHONY: clean
clean: ## Delete lambda-layers/DependenciesLayer/requirements.txt
	rm -rf lambda-layers/DependenciesLayer/requirements.txt

lambda-layers/DependenciesLayer/requirements.txt: uv.lock ## Update the requirements.txt file used to build this Lambda function's DependenciesLayer
	uv export --no-dev > lambda-layers/DependenciesLayer/requirements.txt

requirements.txt: uv.lock ## Update the requirements.txt file used to build this Lambda function's DependenciesLayer
	uv export --no-dev > requirements.txt
