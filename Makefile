generate-js: deps
	@find src -name '*.coffee' | xargs coffee -c -o lib

remove-js:
	@rm -fr lib/

deps:
	@test `which coffee` || echo 'You need to have CoffeeScript in your PATH.\nPlease install it using `brew install coffee-script` or `npm install coffee-script`.'

test: deps
	@find test -name '*_test.coffee' | xargs -n 1 -t coffee

publish: generate-js
	@test `which npm` || echo 'You need npm to do npm publish... makes sense?'
	npm publish
	@remove-js

dev: generate-js
	@coffee -wc --no-wrap -o lib src/*.coffee

.PHONY: all