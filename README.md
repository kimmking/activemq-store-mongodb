ActiveMQ Store MongoDB project
=========

### Introduce  
  This project for creating an ActiveMQ Store by MongoDB.
  If you build all packages from sourcecode with the lastest versions, the xsd files will be auto-generated.
  
### Architecture
    1. modify activemq.xsd in activemq-core.jar, insert mongodb node after kahaDB node:
     <xs:element name='mongodb'>
		<xs:annotation>
			<xs:documentation><![CDATA[
        An implementation of {@link PersistenceAdapter} designed for use with mongodb.
      ]]></xs:documentation>
		</xs:annotation>
		<xs:complexType>
			<xs:sequence>
				<xs:element name='brokerService' minOccurs='0' maxOccurs='1'>
					<xs:complexType>
						<xs:choice minOccurs='0' maxOccurs='1'>
							<xs:element ref='tns:broker' />
							<xs:element ref='tns:brokerService' />
							<xs:any namespace='##other' />
						</xs:choice>
					</xs:complexType>
				</xs:element>
				<xs:element name='usageManager' minOccurs='0' maxOccurs='1'>
					<xs:complexType>
						<xs:choice minOccurs='0' maxOccurs='1'>
							<xs:element ref='tns:systemUsage' />
							<xs:any namespace='##other' />
						</xs:choice>
					</xs:complexType>
				</xs:element>
				<xs:any namespace='##other' minOccurs='0' maxOccurs='unbounded' />
			</xs:sequence>
			<xs:attribute name='archiveDataLogs' type='xs:boolean' />
			<xs:attribute name='brokerName' type='xs:string' />
			<xs:attribute name='brokerService' type='xs:string' />
			<xs:attribute name='checkForCorruptJournalFiles' type='xs:boolean' />
			<xs:attribute name='checkpointInterval' type='xs:long'>
				<xs:annotation>
					<xs:documentation><![CDATA[
            Get the checkpointInterval
          ]]></xs:documentation>
				</xs:annotation>
			</xs:attribute>
			<xs:attribute name='checksumJournalFiles' type='xs:boolean' />
			<xs:attribute name='cleanupInterval' type='xs:long'>
				<xs:annotation>
					<xs:documentation><![CDATA[
            Get the cleanupInterval
          ]]></xs:documentation>
				</xs:annotation>
			</xs:attribute>
			<xs:attribute name='concurrentStoreAndDispatchQueues'
				type='xs:boolean' />
			<xs:attribute name='concurrentStoreAndDispatchTopics'
				type='xs:boolean' />
			<xs:attribute name='databaseLockedWaitDelay' type='xs:integer' />
			<xs:attribute name='host' type='xs:string'>
				<xs:annotation>
					<xs:documentation><![CDATA[
            Get the host
          ]]></xs:documentation>
				</xs:annotation>
			</xs:attribute>
			<xs:attribute name='port' type='xs:integer'>
				<xs:annotation>
					<xs:documentation><![CDATA[
            Get the port
          ]]></xs:documentation>
				</xs:annotation>
			</xs:attribute>
			<xs:attribute name='db' type='xs:string'>
				<xs:annotation>
					<xs:documentation><![CDATA[
            Get the db
          ]]></xs:documentation>
				</xs:annotation>
			</xs:attribute>
		</xs:complexType>
	</xs:element>

    2. add mongodb in META-INF/services/org/apache/xbean/spring/http/activemq.apache.org/schema/core in activemq-core.jar:
       mongodb = org.qsoft.activemq.store.mongodb.MongodbPersistenceAdapter

    3. configure your mongodb server in your activemq.xml:
        <persistenceAdapter>
            <mongodb host="127.0.0.1" port="27017" db="activemq" />
        </persistenceAdapter>
    4. package this project to a jar to reference in your project 
