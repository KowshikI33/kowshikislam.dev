Imports RabbitMQ.Client
Imports System.Text
Imports System.Threading.Tasks

Class Worker
    Public Property NodeRole As String

    Public Sub New(nodeRole As String)
        Me.NodeRole = nodeRole
    End Sub

    Public Async Function StartListeningAsync() As Task
        Dim factory As New ConnectionFactory() With {
            .HostName = "localhost" ' Change host if needed
        }

        Using connection = factory.CreateConnection()
            Using channel = connection.CreateModel()
                Dim queues As String() = {"A", "B", "C", "D", "E"}

                While True
                    For Each queue In queues
                        If Not NodeRole.Contains(queue) Then
                            Console.WriteLine($"Skipping queue {queue} as it's not in the role.")
                            Continue For
                        End If

                        channel.QueueDeclare(queue:=queue, durable:=False, exclusive:=False, autoDelete:=False, arguments:=Nothing)

                        Dim result = channel.BasicGet(queue, True) ' True for auto-acknowledge
                        If result IsNot Nothing Then
                            Dim message As String = Encoding.UTF8.GetString(result.Body.ToArray())
                            Console.WriteLine($" [x] Received '{message}' from queue {queue}")

                            ' Here, handle the message as needed

                            Exit For ' Break out of the For Each loop to start from queue A again
                        End If
                    Next

                    ' Optional: Add a delay between iterations to reduce CPU usage
                    Await Task.Delay(1000)
                End While
            End Using
        End Using
    End Function
End Class

Module Program
    Async Sub Main(args As String())
        Dim nodeRole As String = "A C E" ' Set this based on the node's role

        Dim worker As New Worker(nodeRole)
        Await worker.StartListeningAsync()
    End Sub
End Module
