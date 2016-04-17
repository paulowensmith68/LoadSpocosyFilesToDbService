Imports System.Xml
Imports System.IO
Imports System.Text
Imports MySql.Data
Imports MySql.Data.MySqlClient

Public Class LocalScoposyFile

    ' Holds the connection string to the database used.
    Public connectionString As String = globalConnectionString

    ' Holds message received back from class
    Public returnMessage As String = ""

    Private NodeDatabaseList As New List(Of NodeClass)()

    'Vars used for output message
    Private insertCount As Integer = 0
    Private updateCount As Integer = 0

    'List that hold info on which nodes and attributes to use.
    'These lists are populated in populateLists() function
    Private nodeList As String = ""
    Private attribList As New Dictionary(Of String, String)()

    'ID of XML file. This id is generated after push when xml file is saved.
    Public id As Integer = 0

    'String that holds the xml data. Only used in push. When parsed myXml is used.
    Public xmlData As String = ""

    'XmlDocument var that is used for parsing
    Public myXmlList As New List(Of XmlDocument())

    ' Vars used to control cursor
    Public intCursorCount As Integer = 0

    ' List of filenames to process
    Dim fileIdList As New List(Of Integer)

    'XmlDocument var that is used for parsing
    Dim myXml As New XmlDocument()

    'Load XML Data from file
    Public Sub loadXML(inid As Integer)
        Me.id = inid
        If Not File.Exists(My.Settings.LocalDownloadPath + Me.id.ToString + ".xml") Then
            xmlDone(False)
            Return
        End If
        Try
            Me.myXml.Load(My.Settings.LocalDownloadPath + Me.id.ToString + ".xml")
        Catch
            'Invalid XML Data
            Return
        End Try
    End Sub

    'Parse and save the XML Data
    Public Sub parseData()
        populateLists()
        For Each node As XmlNode In Me.myXml.ChildNodes
            nodeLoop(node, 0)
        Next

        'Write to database 
        If insertCount > 0 Then
            ' Store data in table
            writeDatabase()
        End If

        ' Remove savel_xml entry
        xmlDone(True)

    End Sub

    'Loops through all nodes in the XML file.
    'This function loops itself to traverse through the XML tree.
    Private Sub nodeLoop(node As XmlNode, lvl As Integer)
        'Keep count
        insertCount += 1

        'Parse node if the name is contained in defined nodeList
        If Me.inList(Me.nodeList, node.Name) Then
            parseNode(node)
        End If

        'Write to database 
        If insertCount >= My.Settings.CommitInsertsEvery Then

            ' Store data in table
            writeDatabase()

            NodeDatabaseList.Clear()
            insertCount = 0
        End If

        'Loop through childNodes of current node
        For Each childNode As XmlNode In node.ChildNodes

            nodeLoop(childNode, lvl + 1)
        Next
    End Sub

    Private Sub parseNode(node As XmlNode)
        Dim newListItem As New NodeClass()

        'Hardcode to switch name "event_participant" to "event_participants"
        Dim parseNodeName As String = If((node.Name = "event_participant"), "event_participants", node.Name)
        Dim parse_n_xml As Integer = Convert.ToInt32(node.Attributes("n").Value)
        Dim parse_xmlData As String = "<spocosy version=""1.0"">" + node.OuterXml + "</spocosy>"

        Dim parse_event_id As Integer = 0
        Dim parse_outcome_id As Integer = 0

        ' Extract keys - event
        If parseNodeName = "event" Then
            parse_event_id = Convert.ToInt32(node.Attributes("id").Value)
        End If

        ' outcome
        If parseNodeName = "outcome" Then
            parse_outcome_id = Convert.ToInt32(node.Attributes("id").Value)

            If node.Attributes("object") IsNot Nothing Then
                If node.Attributes("object").Value = "event" Then
                    parse_event_id = Convert.ToInt32(node.Attributes("objectFK").Value)
                End If
            End If
        End If

        ' bettingoffer
        If parseNodeName = "bettingoffer" Then
            If node.Attributes("outcomeFK") IsNot Nothing Then
                parse_outcome_id = Convert.ToInt32(node.Attributes("outcomeFK").Value)
            End If
        End If

        ' Store in the list array
        newListItem = New NodeClass() With {
            .nodeName = parseNodeName,
            .xmlData = parse_xmlData,
            .event_id = parse_event_id,
            .outcome_id = parse_outcome_id,
            .node_n = parse_n_xml
    }

        ' Add to list
        NodeDatabaseList.Add(newListItem)
    End Sub

    'Loops through all nodes in the XML file.
    'This function loops itself to traverse through the XML tree.
    Private Sub writeDatabase()

        Dim cno As New MySqlConnection()
        Dim cmd_del As New MySqlCommand()
        Dim cmd As New MySqlCommand()
        Dim SQLtrans As MySqlTransaction = Nothing
        Dim num As Integer = 0
        Dim i As Integer = 0
        Dim msg As String = ""

        'Hard coding the connString this way is bad, but hopefully informative here.
        cno.ConnectionString = Me.connectionString

        ' Establish insert command
        cmd.Connection = cno
        cmd.Parameters.Add("@nodeName", MySqlDbType.[String])
        cmd.Parameters.Add("@xmlData", MySqlDbType.[String])
        cmd.Parameters.Add("@event_id", MySqlDbType.Int32)
        cmd.Parameters.Add("@outcome_id", MySqlDbType.Int32)
        cmd.Parameters.Add("@node_n", MySqlDbType.Int32)
        cmd.CommandText = "INSERT INTO `oddsmatching`.`bookmaker_xml_nodes` (`nodeName`,`xmlData`,`event_id`,`outcome_id`,`node_n`) VALUES ( @nodeName,@xmlData,@event_id,@outcome_id,@node_n)"

        num = 0

        Try
            cno.Open()
            'Must open connection before starting transaction.
            SQLtrans = cno.BeginTransaction()
            cmd.Transaction = SQLtrans

            Try

                'Ok, this is where the inserts really take place. All the stuff around
                'is just to prepare for this and handle errors that may occur.
                For i = 0 To NodeDatabaseList.Count - 1

                    cmd.Parameters("@nodeName").Value = NodeDatabaseList(i).nodeName
                    cmd.Parameters("@xmlData").Value = NodeDatabaseList(i).xmlData
                    cmd.Parameters("@event_id").Value = NodeDatabaseList(i).event_id
                    cmd.Parameters("@outcome_id").Value = NodeDatabaseList(i).outcome_id
                    cmd.Parameters("@node_n").Value = NodeDatabaseList(i).node_n
                    cmd.ExecuteNonQuery()
                Next
                'We are done. Now commit the transaction - actually change the DB.
                SQLtrans.Commit()
            Catch e1 As System.Exception
                'If anything went wrong attempt to rollback transaction
                Try
                    SQLtrans.Rollback()
                Catch e2 As System.Exception
                End Try
            End Try
        Catch e3 As System.Exception
        Finally
            Try
                'Whatever happens, you will land here and attempt to close the connection.
                cno.Close()
            Catch e4 As System.Exception
            End Try
        End Try
        Me.returnMessage = "SUCCESS: " + NodeDatabaseList.Count.ToString() + " inserted from " + Me.id.ToString + ".xml"

    End Sub

    'Called when XML file is done parsing.
    Private Sub xmlDone(isSuccess As Boolean)
        'Delete id from saved_xml (log table)
        Dim myConnection As New MySqlConnection(Me.connectionString)
        Dim myCommand As New MySqlCommand("delete from saved_streammed_xml where id=@id")
        myCommand.CommandType = CommandType.Text
        myCommand.Connection = myConnection
        myCommand.Parameters.Add(New MySqlParameter("id", Me.id))
        myConnection.Open()
        myCommand.ExecuteNonQuery()

        myConnection.Close()

        Try
            'Move the XML file. (if success to "parsed" else to "error")
            File.Move(My.Settings.LocalDownloadPath + Me.id.ToString + ".xml", My.Settings.LocalDownloadPath + (If((isSuccess), "parsed", "error")) + "\" + Me.id.ToString + ".xml")
        Catch
        End Try
    End Sub

    'Gets all new XML files into collection
    Public Function newXml() As List(Of LocalScoposyFile)
        Dim tempList As New List(Of LocalScoposyFile)()
        Dim myConnection As New MySqlConnection(Me.connectionString)
        Dim myCommand As New MySqlCommand()
        If My.Settings.ProcessStreamsSwitch Then
            myCommand.CommandText = "Select id from saved_streammed_xml where `stream` =@stream"
            myCommand.Parameters.Add("@stream", MySqlDbType.Int32)
            myCommand.Parameters("@stream").Value = My.Settings.ProcessStreamNumber
        Else
            myCommand.CommandText = "Select id from saved_streammed_xml"

        End If
        myCommand.CommandType = CommandType.Text
        myCommand.Connection = myConnection

        myConnection.Open()
        Dim myReader As MySqlDataReader = myCommand.ExecuteReader()

        Dim intLoop As Integer = 0
        While myReader.Read()
            intLoop += 1
            ' Add one
            Dim tmp As New LocalScoposyFile()
            tmp.loadXML(Convert.ToInt32(myReader("id")))
            tempList.Add(tmp)

            ' For memory purposes
            If intLoop >= My.Settings.MaxFilesToLoad Then
                Exit While
            End If

        End While

        myConnection.Close()
        myReader.Dispose()
        myCommand.Dispose()

        Return tempList
    End Function


    'Populate lists
    Private Sub populateLists()
        'Only these nodes should be parsed
        ' Changed removed result  Me.nodeList = "event_participant,country,status_desc,result_type,incident_type,event_incident_type,event_incident_type_text,lineup_type,offence_type,standing_type,standing_type_param,standing_config,language_type,sport,participant,tournament_template,tournament,tournament_stage,event,event_participants,outcome,bettingoffer,object_participants,lineup,incident,event_incident,event_incident_detail,result,standing,standing_participants,standing_data,property,language,image,reference,reference_type,odds_provider,scope_type,scope_data_type,event_scope,event_scope_detail,scope_result,lineup_scope_result,venue_data,venue_data_type,venue,venue_type"
        Me.nodeList = "event_participant,country,status_desc,result_type,incident_type,event_incident_type,event_incident_type_text,lineup_type,offence_type,standing_type,standing_type_param,standing_config,language_type,sport,participant,tournament_template,tournament,tournament_stage,event,event_participants,outcome,bettingoffer,lineup,incident,event_incident,event_incident_detail,standing,standing_participants,standing_data,property,language,image,reference,reference_type,odds_provider,scope_type,scope_data_type,event_scope,event_scope_detail,scope_result,lineup_scope_result,venue_data,venue_data_type,venue,venue_type"

        'Attributes that should always be included
        Me.attribList.Add("ALL", "id,n,ut,del")

        '
        '             * List of tables, which each contain a list of attributes, remove or add
        '             * attributes here to have it inculded in the database 
        '             * (make sure it the field exists in the database when adding attributes)
        '             

        Me.attribList.Add("bettingoffer", "outcomeFK,odds_providerFK,odds,odds_old,active,is_back,is_single,is_live,volume,currency,couponKey")
        Me.attribList.Add("country", "name")
        Me.attribList.Add("event", "name,tournament_stageFK,startdate,eventstatusFK,status_type,status_descFK")
        Me.attribList.Add("event_incident", "eventFK,sportFK,event_incident_typeFK,elapsed,elapsed_plus,comment,sortorder")
        Me.attribList.Add("event_incident_detail", "type,event_incidentFK,participantFK,value")
        Me.attribList.Add("event_incident_type", "player1,player2,team,comment,subtype1,subtype2,name,type,comment_type,player2_type")
        Me.attribList.Add("event_incident_type_text", "event_incident_typeFK,name")
        Me.attribList.Add("event_participants", "number,participantFK,eventFK")
        Me.attribList.Add("image", "object,objectFK,type,contenttype,name,value")
        Me.attribList.Add("incident", "event_participantsFK,incident_typeFK,incident_code,elapsed,sortorder,ref_participantFK")
        Me.attribList.Add("incident_type", "name,subtype")
        Me.attribList.Add("language", "object,objectFK,language_typeFK,name")
        Me.attribList.Add("language_type", "name,description")
        Me.attribList.Add("lineup", "event_participantsFK,participantFK,lineup_typeFK,shirt_number,pos")
        Me.attribList.Add("lineup_type", "name")
        Me.attribList.Add("object_participants", "object,objectFK,participantFK,participant_type,active")
        Me.attribList.Add("offence_type", "name")
        Me.attribList.Add("odds_provider", "name,url,bookmaker,preferred,betex,active")
        Me.attribList.Add("outcome", "object,objectFK,type,event_participant_number,scope,subtype,iparam,iparam2,dparam,dparam2,sparam")
        Me.attribList.Add("participant", "name,gender,type,countryFK,enetID,enetSportID")
        Me.attribList.Add("property", "object,objectFK,type,name,value")
        Me.attribList.Add("reference", "object,objectFK,refers_to,name")
        Me.attribList.Add("reference_type", "name,description")
        Me.attribList.Add("result", "event_participantsFK,result_typeFK,result_code,value")
        Me.attribList.Add("result_type", "name,code")
        Me.attribList.Add("sport", "name")
        Me.attribList.Add("standing", "object,objectFK,standing_typeFK,name")
        Me.attribList.Add("standing_config", "standingFK,standing_type_paramFK,value,sub_param")
        Me.attribList.Add("standing_data", "standing_participantsFK,standing_type_paramFK,value,code,sub_param")
        Me.attribList.Add("standing_participants", "standingFK,participantFK,rank")
        Me.attribList.Add("standing_type", "name,description")
        Me.attribList.Add("standing_type_param", "standing_typeFK,code,name,type,value")
        Me.attribList.Add("status_desc", "name,status_type")
        Me.attribList.Add("tournament", "name,tournament_templateFK")
        Me.attribList.Add("tournament_stage", "name,tournamentFK,gender,countryFK,startdate,enddate")
        Me.attribList.Add("tournament_template", "name,sportFK,gender")
        Me.attribList.Add("scope_type", "name,description")
        Me.attribList.Add("scope_data_type", "name,description")
        Me.attribList.Add("event_scope", "eventFK,scope_typeFK")
        Me.attribList.Add("event_scope_detail", "event_scopeFK,name,value")
        Me.attribList.Add("scope_result", "event_participantsFK,event_scopeFK,scope_data_typeFK,value")
        Me.attribList.Add("lineup_scope_result", "lineupFK,event_scopeFK,scope_data_typeFK,value")
        Me.attribList.Add("venue_data", "value,venue_data_typeFK,venueFK")
        Me.attribList.Add("venue_data_type", "name")
        Me.attribList.Add("venue", "name,countryFK,venue_typeFK")
        Me.attribList.Add("venue_type", "name")
    End Sub

    'Helper functions
    'Check if value is in comma seperated list
    Private Function inList(list As String, checkString As String) As Boolean
        Return (list.StartsWith(checkString) OrElse list.IndexOf(Convert.ToString(",") & checkString) <> -1)
    End Function


End Class
