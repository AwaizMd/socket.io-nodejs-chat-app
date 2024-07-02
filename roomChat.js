const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const { createClient } = require('redis');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const { Pool } = require('pg');
const cron = require('node-cron');

const app = express();
const server = http.createServer(app);
const io = socketIo(server);

// Create Redis client
const redisClient = createClient({
    url: 'redis://localhost:6379'
});

redisClient.on('error', (err) => {
    console.error('Redis Client Error', err);
});

redisClient.connect().catch(console.error);

app.use(express.json());

// JWT Secret
const JWT_SECRET = 'my_jwt_secret';

// PostgreSQL connection
const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'postgres',
  password: 'wyliwyg@1',
  port: 5432,
});

pool.connect()
  .then(() => console.log('Connected to PostgreSQL'))
  .catch(err => console.error('Connection error', err.stack));

// Register route
app.post('/register', async (req, res) => {
  const { username, password } = req.body;
  const hashedPassword = await bcrypt.hash(password, 10);
  console.log("body: ",req.body);
  try {
    const result = await pool.query(
      'INSERT INTO users (username, password) VALUES ($1, $2) RETURNING *',
      [username, hashedPassword]
    );
    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.log(error);
    res.status(400).json({ error: 'User already exists' });
  }
});

// Login route
app.post('/login', async (req, res) => {
  const { username, password } = req.body;
  try {
    const result = await pool.query('SELECT * FROM users WHERE username = $1', [username]);
    const user = result.rows[0];
    if (user && (await bcrypt.compare(password, user.password))) {
      const token = jwt.sign({ userId: user.id }, JWT_SECRET);
      res.json({ token });
    } else {
      res.status(401).json({ error: 'Invalid credentials' });
    }
  } catch (error) {
    res.status(500).json({ error: 'Database error' });
  }
});

// Middleware to authenticate JWT tokens
const authenticateJWT = (req, res, next) => {
  const authHeader = req.header('Authorization');
  if (authHeader) {
    jwt.verify(authHeader, JWT_SECRET, (err, user) => {
      if (err) {
        return res.sendStatus(403);
      }
      req.user = user;
      next();
    });
  } else {
    res.sendStatus(401);
  }
};

app.get('/profile', authenticateJWT, async (req, res) => {
  try {
    const result = await pool.query('SELECT id, username FROM users WHERE id = $1', [req.user.userId]);
    res.json(result.rows[0]);
  } catch (error) {
    res.status(500).json({ error: 'Database error' });
  }
});

/* GET messages endpoint with pagination in final result */
app.get('/messages', authenticateJWT, async (req, res) => {
    const { userId, roomId, page = 1, pageSize = 10 } = req.query;
    const offset = (page - 1) * pageSize;
    
    try {
      /* Fetch messages from Redis */
      const redisMessages = await redisClient.lRange('messages', 0, -1);
      const parsedRedisMessages = redisMessages.map(JSON.parse);
      
      /* Filter Redis messages by userId and roomId */
      const filteredRedisMessages = parsedRedisMessages.filter(message =>
        message.userId === userId && message.roomId === roomId
      );
  
      /* Fetch messages from PostgreSQL */
      const postgresMessagesResult = await pool.query(
        'SELECT * FROM messages WHERE userId = $1 AND roomId = $2 ORDER BY timestamp ASC',
        [userId, roomId]
      );
      const postgresMessages = postgresMessagesResult.rows;
  
      /* Combine Redis and PostgreSQL messages */
      let allMessages = [...filteredRedisMessages, ...postgresMessages];
      
      /* Sort messages by timestamp */
      allMessages.sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
      
      /* Apply pagination to combined messages */
      const paginatedMessages = allMessages.slice(offset, offset + pageSize);
  
      res.json(paginatedMessages);
    } catch (error) {
      console.error('Error fetching messages:', error);
      res.status(500).json({ error: 'Failed to fetch messages' });
    }
});
  
  

/* 
  The purpose of this middleware is to ensure that only authenticated users can establish a socket connection
*/
io.use((socket, next) => {
  const token = socket.handshake.headers.token;
  if (token) {
    jwt.verify(token, JWT_SECRET, (err, user) => {
      if (err) {
        return next(new Error('Authentication error'));
      }
      socket.user = user;
      next();
    });
  } else {
    next(new Error('Authentication error'));
  }
});

/* joining a room  */
io.on('connection', (socket) => {

  /* Extract roomId from the query parameters sent during connection handshake */
  const roomId = socket.handshake.query.roomId;

  /* Join the socket to the specified room based on the extracted roomId */
  socket.join(roomId);
  console.log(`Client connected to room ${roomId}`, socket.id);

  /* Event handler for 'sendMessage' event */
  socket.on('sendMessage', async (messageContent) => {
    const message = {
      content: messageContent,
      userId: socket.user.userId,
      timestamp: new Date().toISOString(),
      roomId: roomId
    };
    console.log("message: ", message);
    try {
      await redisClient.rPush(`messages`, JSON.stringify(message));
      io.to(roomId).emit('receiveMessage', message);
    } catch (err) {
      console.error('Error pushing message to Redis:', err);
    }
  });

  socket.on('updateMessage', async ({ messageId, updatedContent }) => {
    try {
      const messages = await redisClient.lRange(`messages`, 0, -1);
      const updatedMessages = messages.map((message) => {
        const parsedMessage = JSON.parse(message);
        if (parsedMessage.id === messageId) {
          parsedMessage.content = updatedContent;
        }
        return JSON.stringify(parsedMessage);
      });

      await redisClient.del(`messages`);
      await redisClient.rPush(`messages`, updatedMessages);
      const updatedMessage = updatedMessages.find((message) => JSON.parse(message).id === messageId);
      io.to(roomId).emit('messageUpdated', JSON.parse(updatedMessage));
    } catch (err) {
      console.error('Error updating message in Redis:', err);
    }
  });


  socket.on('disconnect', () => {
    console.log('Client disconnected');
  });
});

const dumpDataToPostgres = async () => {
    try {
      const messages = await redisClient.lRange('messages', 0, -1);
  
      if (messages.length === 0) {
        console.log('No messages to dump');
        return;
      }
  
      const parsedMessages = messages.map(JSON.parse);
  
      // Prepare bulk insert query
      const values = [];
      const placeholders = parsedMessages.map((_, i) => {
        const offset = i * 3;
        values.push(parsedMessages[i].content, parsedMessages[i].userId, parsedMessages[i].timestamp);
        return `($${offset + 1}, $${offset + 2}, $${offset + 3})`;
      }).join(',');

      console.log("placeholders: ",placeholders);
  
      const insertQuery = `INSERT INTO messages (content, userId, timestamp) VALUES ${placeholders}`;
  
      try {
        const client = await pool.connect();
        try {
          await client.query('BEGIN');
          await client.query(insertQuery, values);
          await client.query('COMMIT');
          await redisClient.del('messages');
          console.log('Dumped messages to PostgreSQL');
        } catch (error) {
          await client.query('ROLLBACK');
          console.error('Error saving messages to PostgreSQL:', error);
        } finally {
          client.release();
        }
      } catch (error) {
        console.error('Error connecting to PostgreSQL:', error);
      }
    } catch (err) {
      console.error('Error fetching messages from Redis:', err);
    }
  };
  

// Schedule the dump to run every 15 minutes
cron.schedule('*/5 * * * *', () => {
  console.log('Running cron job to dump data from Redis to PostgreSQL');
  dumpDataToPostgres();
});

server.listen(8080, () => console.log('Server is running on port 8080'));
