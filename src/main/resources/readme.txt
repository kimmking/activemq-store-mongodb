readme

两种使用方式：
1、直接使用activemq-all-5.5.0-mongo-0.1.jar
2、修改你所用的activemq 5.x版本
为了使activemq在配置文件中支持mongodb，需要修改activemq的jar中的两个配置文件。
修改后的文件可以在activemq-all-5.5.0-mongo-0.1.jar中找到。
两个文件是：
activemq.xsd  添加了对mongodb节点的schema定义
META-INF\services\org\apache\xbean\spring\http\activemq.apache.org\schema\core 添加了mongodb的实现类
