javac -cp ".:/usr/local/Thrift/*" SuperNodeServer.java -d .
javac -cp ".:/usr/local/Thrift/*" NodeServer.java -d .
javac -cp ".:/usr/local/Thrift/*" Client.java -d .



java -cp ".:/usr/local/Thrift/*" SuperNodeServer 1729 9

