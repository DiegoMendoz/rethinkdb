const express = require('express');
const http = require('http');
const cors = require('cors');
const rethinkdb = require('rethinkdb');
const WebSocket = require('ws');
const app = express();
const wsApp = express();

// Middleware para parsear JSON y habilitar CORS
app.use(express.json());
app.use(cors());

<<<<<<< HEAD
// Variable para almacenar la conexión a RethinkDB
let connection = null;

// Conectar a RethinkDB y asegurar que la base de datos y la tabla existan
r.connect({ host: 'rethinkdb', port: 28015 }, async (err, conn) => {
=======
const webSocketServer = http.createServer(wsApp);
const wss = new WebSocket.Server({ server: webSocketServer });

wss.on('connection', ws => {
    ws.uid = Math.random().toString(36);
    console.log('WebSocket connection established with user: ', ws.uid);
    ws.on('message', message => {
        console.log('Received message:', message.stringify.JSON);
    });
    ws.on('close', () => {
        console.log('WebSocket connection closed');
    });
});

const broadcast = (data) => {
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(data));
        }
    });
}

let connection = null;

rethinkdb.connect({ host: 'rethinkdb', port: 28015 }, async (err, conn) => {
>>>>>>> 8f7e71be466a8ef7c19e35edc85a574553d768fe
    if (err) throw err;
    connection = conn;
    await ensureDbAndTable(conn, 'test', 'comentarios');
    monitorChanges(conn);//connection to websocket changes
});

// Función para asegurar que la base de datos y la tabla existan
async function ensureDbAndTable(conn, dbName, tableName) {
    const dbExists = await rethinkdb.dbList().contains(dbName).run(conn);
    if (!dbExists) {
        await rethinkdb.dbCreate(dbName).run(conn);
    }
    const tableExists = await rethinkdb.db(dbName).tableList().contains(tableName).run(conn);
    if (!tableExists) {
        await rethinkdb.db(dbName).tableCreate(tableName).run(conn);
    }
}

// Ruta para insertar datos en la base de datos
app.post('/insert', (req, res) => {
    const { db, table, data } = req.body;
    rethinkdb.db(db).table(table).insert(data)
        .run(connection, (err, result) => {
            if (err) {
                console.error('Error inserting data:', err);
                res.status(500).json({ error: err.message }); // Respond with error message
            } else {
                console.log('Data inserted successfully:', result);
                res.status(200).json({ success: true }); // Respond with success message
            }
        });
});

// Ruta para obtener datos de la base de datos
app.get('/get', (req, res) => {
    const { db, table } = req.query;
    rethinkdb.db(db).table(table).run(connection, (err, cursor) => {
        if (err) res.status(500).send(err);
        else {
            cursor.toArray((err, result) => {
                if (err) res.status(500).send(err);
                else res.send(result);
            });
        }
    });
});

app.delete('/delete/:id', (req, res) => {
    const { db, table } = req.query;
    const commentId = req.params.id; // Obtiene el ID del comentario desde el URL

    rethinkdb.db(db).table(table).get(commentId).delete({ returnChanges: true })
        .run(connection, (err, result) => {
            if (err) {
                console.error('Error deleting comment:', err);
                res.status(500).json({ error: err.message });
            } else if (result.deleted === 0) {
                res.status(404).json({ message: "Comment not found." });
            } else {
                console.log('Comment deleted successfully:', result.changes[0].old_val);
                res.status(200).json(result.changes[0].old_val); // Devuelve el comentario eliminado
            }
        });
});

// Nueva ruta para actualizar comentarios
app.put('/update', (req, res) => {
    const { db, table, data } = req.body;
    const { id, ...updateData } = data;

    rethinkdb.db(db).table(table).get(id).replace({ ...updateData, id })
        .run(connection, (err, result) => {
            if (err) {
                console.error('Error updating data:', err);
                res.status(500).json({ error: err.message });
            } else if (result.replaced === 0) {
                res.status(404).json({ message: "Comment not found." });
            } else {
                console.log('Data updated successfully:', result);
                res.status(200).json({ success: true });
            }
        });
});

const monitorChanges = async (conn) => {
    rethinkdb.db('test').table('comentarios').wait().run(conn, () => {
        console.log('table ready');
        rethinkdb.db('test').table('comentarios').changes().run(conn, (err, cursor) => {
            if (err) throw err;
            cursor.each((err, row) => {
                if (err) throw err;
                console.log('Change detected:', row);
                broadcast(row);
            });
            console.log('monitoring changes to broadcast...')
        });
    })
};



app.listen(3000, () => {
    console.log('Server is running on port 3000');
});

webSocketServer.listen(3001, () => {
    console.log('WebSocket server is running on port 3001');
})
