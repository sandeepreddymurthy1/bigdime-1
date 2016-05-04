module.exports = function (grunt) {
    grunt.initConfig({
            copy: {
                main: {
                    files: [
//                        {expand: true, src: ['app/**'], dest: '../src/main/webapp/'}
                          {expand: true, cwd:'app/' ,src: ['**'], dest: '../src/main/webapp/'}
                    ]
                }
            },
            watch: {
                scripts: {
                    files: ['app/**'],
                    options: {
                        nospawn: true
                    }
                }
            },

            connect: {
                test: {
                    options: {
                        port: 8000,
                        base: '.'
                    }
                },
                development: {
                    options: {
                        port: 8001,
                        base: '.',
                        keepalive: true
                    }
                }
            },

            karma: {
                files: ['app/js/**/ *.js'],
                unit: {
                    configFile: 'config/karma.conf.js',
                    singleRun: true
                },
                e2e: {
                    configFile: 'config/karma-e2e.conf.js',
                    singleRun: true
                }
            }

        }
    )
    ;

    grunt.event.on('watch', function (action, filepath) {
        var destinationPath = "../src/main/webapp/";
        if (action === 'deleted') {
            grunt.log.writeln("WARNING: File " + filepath + " deleted");
        }
        else {
            grunt.log.writeln(filepath + " " + action);
            grunt.file.copy(filepath, destinationPath + filepath);
        }
    });

    grunt.loadNpmTasks('grunt-karma');
    grunt.registerTask('develop', ['copy', 'watch']);
    grunt.registerTask('test', ['connect:test', 'karma:unit', 'karma:e2e']);
    grunt.loadNpmTasks('grunt-contrib-watch');
    grunt.loadNpmTasks('grunt-contrib-connect');
    grunt.loadNpmTasks('grunt-contrib-copy');

}