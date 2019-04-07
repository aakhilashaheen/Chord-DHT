javac -cp ".:/usr/local/Thrift/*" SuperNodeServer.java -d .
javac -cp ".:/usr/local/Thrift/*" NodeServer.java -d .

java -cp ".:/usr/local/Thrift/*" SuperNodeServer 5
