import express from 'express';
import axios from 'axios';
import cors from 'cors';
import path from 'path';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime.js';
import { Sequelize } from 'sequelize';
import dotenv from 'dotenv';

dotenv.config();
dayjs.extend(relativeTime);

const {
    AZURE_SQL_HOST,
    AZURE_SQL_PORT,
    AZURE_SQL_DATABASE,
    AZURE_SQL_USER,
    AZURE_SQL_PASSWORD
} = process.env;

if (!AZURE_SQL_HOST || !AZURE_SQL_DATABASE || !AZURE_SQL_USER) {
    throw new Error('Azure SQL environment variables are not set in .env');
}

export const sequelize = new Sequelize(
    AZURE_SQL_DATABASE,
    AZURE_SQL_USER,
    AZURE_SQL_PASSWORD,
    {
        host: AZURE_SQL_HOST,
        port: AZURE_SQL_PORT ? parseInt(AZURE_SQL_PORT, 10) : 1433,
        dialect: 'mssql',
        dialectOptions: {
            options: {
                encrypt: true,
                trustServerCertificate: false,
            }
        },
        logging: process.env.NODE_ENV === 'development' ? console.log : false,
        pool: {
            max: 5,
            min: 0,
            acquire: 30000,
            idle: 10000
        }
    }
);

const app = express();
const PORT = process.env.PORT || 3000;

const User = sequelize.define('User', {
  id: {
    type: Sequelize.INTEGER,
    primaryKey: true,
    autoIncrement: true,
  },
  fullName: {
    type: Sequelize.STRING,
    allowNull: false,
  },
  username: {
    type: Sequelize.STRING,
    allowNull: false,
    // unique: true, // Removed unique constraint here to avoid SQL Server ALTER COLUMN error
  },
  email: {
    type: Sequelize.STRING,
    allowNull: false,
    // unique: true, // Removed unique constraint here to avoid SQL Server ALTER COLUMN error
  },
  password: {
    type: Sequelize.STRING,
    allowNull: false,
  },
  company: {
    type: Sequelize.STRING,
    allowNull: true,
  },
  phone: {
    type: Sequelize.STRING,
    allowNull: true,
  },
}, {
  timestamps: true,
});

const AuthToken = sequelize.define('AuthToken', {
  id: {
    type: Sequelize.INTEGER,
    primaryKey: true,
    autoIncrement: true,
  },
  userId: {
    type: Sequelize.INTEGER,
    allowNull: false,
  },
  token: {
    type: Sequelize.STRING,
    allowNull: false,
  },
}, {
  timestamps: true,
});

const Notebook = sequelize.define('Notebook', {
  id: {
    type: Sequelize.INTEGER,
    primaryKey: true,
    autoIncrement: true,
  },
  fileName: {
    type: Sequelize.STRING,
    allowNull: false,
  },
  filesize: {
    type: Sequelize.INTEGER,
    allowNull: true,
  },
  status: {
    type: Sequelize.INTEGER,
    allowNull: true,
  },
  total_rows: {
    type: Sequelize.INTEGER,
    allowNull: true,
  },
  taskId: {
    type: Sequelize.STRING,
    allowNull: true,
  },
}, {
  timestamps: true,
});

const Organization = sequelize.define('Organization', {
  id: {
    type: Sequelize.INTEGER,
    primaryKey: true,
    autoIncrement: true,
  },
  name: {
    type: Sequelize.STRING,
    allowNull: false,
  },
  address: {
    type: Sequelize.STRING,
    allowNull: true,
  },
  phone: {
    type: Sequelize.STRING,
    allowNull: true,
  },
  email: {
    type: Sequelize.STRING,
    allowNull: true,
  },
}, {
  timestamps: true,
});

// Automatically migrate tables
sequelize.sync({ alter: true })
  .then(() => {
    console.log('Database tables migrated successfully.');
  })
  .catch((error) => {
    console.error('Error migrating database tables:', error);
  });

const JWT_SECRET = process.env.JWT_SECRET || 'your_jwt_secret';
const JWT_EXPIRES_IN = '1h';

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(process.cwd(), 'dist')));

// Helper function to list notebooks
async function listNotebooks(folderPath) {
  try {
    const response = await axios.get(
      `${process.env.DATABRICKS_INSTANCE}/api/2.0/workspace/list`,
      {
        params: { path: folderPath },
        headers: {
          Authorization: `Bearer ${process.env.DATABRICKS_TOKEN}`
        }
      }
    );

    if (!response.data || !Array.isArray(response.data.objects)) {
      throw new Error("No objects found in the response or invalid response format.");
    }

    const notebooks = response.data.objects.filter(obj => obj.object_type === "NOTEBOOK");
    return notebooks;

  } catch (error) {
    console.error("Failed to list notebooks:", error);
    throw new Error("Failed to list notebooks.");
  }
}

// Databricks notebook APIs

app.post("/run-notebook", async (req, res) => {
  try {
    const { fileName, source } = req.body;
    const databricksToken = process.env.DATABRICKS_TOKEN;
    const databricksInstance = process.env.DATABRICKS_INSTANCE;
    const notebookFolderPath = process.env.NOTEBOOK_PATH;

    if (!source || !databricksToken || !databricksInstance || !notebookFolderPath) {
      return res.status(400).json({ error: "Missing required parameters or environment variables" });
    }

    const notebooks = await listNotebooks(notebookFolderPath);
    const matchingNotebook = notebooks.find(nb =>
      nb.path.toLowerCase().includes(source.toLowerCase())
    );

    if (!matchingNotebook) {
      return res.status(400).json({ error: `No notebook found matching source: ${source}` });
    }

    const response = await axios.post(
      `${databricksInstance}/api/2.1/jobs/runs/submit`,
      {
        run_name: "Triggered from MDC App",
        existing_cluster_id: process.env.DATABRICKS_CLUSTER_ID,
        notebook_task: {
          notebook_path: matchingNotebook.path,
        },
      },
      {
        headers: {
          Authorization: `Bearer ${databricksToken}`,
          "Content-Type": "application/json",
        },
      }
    );

    res.json({
      message: `Notebook execution triggered successfully.`,
      run_id: response.data.run_id,
      notebook_name: matchingNotebook.path,
      notebooks,
    });
  } catch (error) {
    console.error("Failed to trigger notebook:", error);
    res.status(500).json({ error: "Failed to trigger notebook" });
  }
});

app.get("/list-notebooks", async (req, res) => {
  try {
    const notebookPath = process.env.NOTEBOOK_PATH;
    const notebooks = await listNotebooks(notebookPath);
    res.json({
      message: `Notebooks in folder ${notebookPath}:`,
      notebooks,
    });
  } catch (error) {
    console.error("Failed to list notebooks:", error);
    res.status(500).json({ error: "Failed to list notebooks" });
  }
});

app.get("/run-status/:runId", async (req, res) => {
  try {
    const runId = req.params.runId;
    const databricksToken = process.env.DATABRICKS_TOKEN;
    const databricksInstance = process.env.DATABRICKS_INSTANCE;

    if (!runId || !databricksToken || !databricksInstance) {
      return res.status(500).json({ error: "Missing required parameters or environment variables" });
    }

    const response = await axios.get(
      `${databricksInstance}/api/2.1/jobs/runs/get`,
      {
        params: { run_id: runId },
        headers: { Authorization: `Bearer ${databricksToken}` },
      }
    );

    const progressMap = {
      PENDING: 10,
      QUEUED: 20,
      RUNNING: 60,
      TERMINATING: 90,
      TERMINATED: 100,
    };

    const lifeCycle = response.data.state.life_cycle_state;
    const result_state = response.data.state.result_state || 'N/A';
    const state_message = response.data.state.state_message || '';

    res.json({
      message: `Status for run_id ${runId}:`,
      runStatus: response.data.state,
      status: lifeCycle === 'TERMINATED' ? 'completed'
            : lifeCycle === 'INTERNAL_ERROR' ? 'error'
            : lifeCycle.toLowerCase(),
      message: state_message,
      result: result_state,
      progress: progressMap[lifeCycle] || 0,
    });

  } catch (error) {
    console.error("Failed to fetch run status:", error);
    res.status(500).json({ error: "Failed to fetch run status" });
  }
});

// Auth APIs

app.post('/auth/register', async (req, res) => {
  try {
    const { username, email, password } = req.body;
    if (!username || !email || !password) {
      return res.status(400).json({ message: 'All fields are required.' });
    }

    const existingUser = await User.findOne({ where: { email } });
    if (existingUser) {
      return res.status(409).json({ message: 'Email already registered.' });
    }

    const user = await User.create({ username, email, password });
    return res.status(201).json({ message: 'User registered successfully.', user: { id: user.id, username, email } });
  } catch (error) {
    console.error("Registration failed:", error);
    return res.status(500).json({ message: 'Registration failed.', error: error.message });
  }
});

app.post('/auth/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password) {
      return res.status(400).json({ message: 'Email and password are required.' });
    }

    const user = await User.findOne({ where: { email } });
    if (!user) {
      return res.status(401).json({ message: 'Invalid credentials.' });
    }

    const isMatch = await bcrypt.compare(password, user.password);
    if (!isMatch) {
      return res.status(401).json({ message: 'Invalid credentials.' });
    }

    const token = jwt.sign({ id: user.id, email: user.email }, JWT_SECRET, { expiresIn: JWT_EXPIRES_IN });
    await AuthToken.create({ userId: user.id, token });

    return res.json({ token, user: { id: user.id, username: user.username, email: user.email } });
  } catch (error) {
    console.error("Login failed:", error);
    return res.status(500).json({ message: 'Login failed.', error: error.message });
  }
});

app.post('/auth/logout', async (req, res) => {
  try {
    const token = req.headers.authorization?.split(' ')[1];
    if (!token) {
      return res.status(400).json({ message: 'Token required.' });
    }

    await AuthToken.destroy({ where: { token } });
    return res.json({ message: 'Logged out successfully.' });
  } catch (error) {
    console.error("Logout failed:", error);
    return res.status(500).json({ message: 'Logout failed.', error: error.message });
  }
});

app.post('/auth/token', async (req, res) => {
  try {
    const { token } = req.body;
    if (!token) {
      return res.status(400).json({ message: 'Token required.' });
    }

    jwt.verify(token, JWT_SECRET, (err, decoded) => {
      if (err) {
        return res.status(401).json({ message: 'Invalid token.' });
      }
      const newToken = jwt.sign({ id: decoded.id, email: decoded.email }, JWT_SECRET, { expiresIn: JWT_EXPIRES_IN });
      res.json({ token: newToken });
    });
  } catch (error) {
    console.error("Token refresh failed:", error);
    return res.status(500).json({ message: 'Token refresh failed.', error: error.message });
  }
});

// Dashboard APIs

app.get('/dashboard/metrics', async (req, res) => {
  try {
    const totalUploads = await Notebook.count();
    const successfulUploads = await Notebook.count({ where: { status: 1 } });
    const failedUploads = await Notebook.count({ where: { status: 0 } });
    const dataProcessedResult = await Notebook.findAll({
      attributes: [
        [Notebook.sequelize.fn('SUM', Notebook.sequelize.col('total_rows')), 'totalRowsProcessed']
      ],
      raw: true
    });
    const dataProcessed = dataProcessedResult[0].totalRowsProcessed || 0;

    const recentUploadsRaw = await Notebook.findAll({
      order: [['createdAt', 'DESC']],
      limit: 5,
      attributes: ['fileName', 'filesize', 'createdAt'],
      raw: true
    });

    const recentUploads = recentUploadsRaw.map(upload => ({
      fileName: upload.fileName,
      filesize: upload.filesize,
      timeAgo: dayjs(upload.createdAt).fromNow()
    }));

    res.json({
      totalUploads,
      successfulUploads,
      failedUploads,
      dataProcessed,
      recentUploads
    });
  } catch (error) {
    console.error("Error fetching dashboard metrics:", error);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// Organization APIs

app.post('/organizations', async (req, res) => {
  try {
    const organizationData = req.body;
    const newOrganization = await Organization.create(organizationData);
    res.status(201).json(newOrganization);
  } catch (error) {
    res.status(500).json({ message: 'Error creating organization', error });
  }
});

app.get('/organizations', async (req, res) => {
  try {
    const organizations = await Organization.findAll();
    res.status(200).json(organizations);
  } catch (error) {
    res.status(500).json({ message: 'Error retrieving organizations', error });
  }
});

app.get('/organizations/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const organization = await Organization.findByPk(id);
    if (!organization) {
      return res.status(404).json({ message: 'Organization not found' });
    }
    res.status(200).json(organization);
  } catch (error) {
    res.status(500).json({ message: 'Error retrieving organization', error });
  }
});

app.put('/organizations/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const organizationData = req.body;
    const organization = await Organization.findByPk(id);
    if (!organization) {
      return res.status(404).json({ message: 'Organization not found' });
    }
    await organization.update(organizationData);
    res.status(200).json(organization);
  } catch (error) {
    res.status(500).json({ message: 'Error updating organization', error });
  }
});

app.delete('/organizations/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const organization = await Organization.findByPk(id);
    if (!organization) {
      return res.status(404).json({ message: 'Organization not found' });
    }
    await organization.destroy();
    res.status(204).send();
  } catch (error) {
    res.status(500).json({ message: 'Error deleting organization', error });
  }
});

// User APIs

app.post('/users', async (req, res) => {
  try {
    const { fullName, email, password, company, phone } = req.body;
    if (!fullName || !email || !password) {
      return res.status(400).json({ message: 'All fields are required.' });
    }
    const username = email.split('@')[0];

    const existingUser = await User.findOne({ where: { email } });
    if (existingUser) {
      return res.status(409).json({ message: 'Email already registered.' });
    }

    const user = await User.create({ fullName, username, email, password, company, phone });
    res.status(201).json({ message: 'User created successfully.', user: { id: user.id, fullName, username, email, company, phone } });
  } catch (error) {
    res.status(500).json({ message: 'User creation failed.', error: error.message });
  }
});

app.get('/users', async (req, res) => {
  try {
    const users = await User.findAll({ attributes: { exclude: ['password'] } });
    res.json(users);
  } catch (error) {
    res.status(500).json({ message: 'Failed to retrieve users.', error: error.message });
  }
});

app.get('/users/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const user = await User.findByPk(id, { attributes: { exclude: ['password'] } });
    if (!user) {
      return res.status(404).json({ message: 'User not found.' });
    }
    res.json(user);
  } catch (error) {
    res.status(500).json({ message: 'Failed to retrieve user.', error: error.message });
  }
});

app.put('/users/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const { fullName, username, email, password, company, phone } = req.body;
    const user = await User.findByPk(id);
    if (!user) {
      return res.status(404).json({ message: 'User not found.' });
    }

    if (fullName) user.fullName = fullName;
    if (username) user.username = username;
    if (email) user.email = email;
    if (password) user.password = password;
    if (company) user.company = company;
    if (phone) user.phone = phone;

    await user.save();

    res.json({ message: 'User updated successfully.', user: { id: user.id, fullName: user.fullName, username: user.username, email: user.email, company: user.company, phone: user.phone } });
  } catch (error) {
    res.status(500).json({ message: 'User update failed.', error: error.message });
  }
});

app.delete('/users/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const user = await User.findByPk(id);
    if (!user) {
      return res.status(404).json({ message: 'User not found.' });
    }
    await user.destroy();
    res.json({ message: 'User deleted successfully.' });
  } catch (error) {
    res.status(500).json({ message: 'User deletion failed.', error: error.message });
  }
});

app.post('/users/change-password', async (req, res) => {
  try {
    const userId = req.user?.id || req.body.id;
    const { currentPassword, newPassword } = req.body;

    if (!currentPassword || !newPassword) {
      return res.status(400).json({ message: 'Current and new passwords are required.' });
    }

    const user = await User.findByPk(userId);
    if (!user) {
      return res.status(404).json({ message: 'User not found.' });
    }

    const passwordMatch = await bcrypt.compare(currentPassword, user.password);
    if (!passwordMatch) {
      return res.status(401).json({ message: 'Current password is incorrect.' });
    }

    user.password = newPassword;
    await user.save();

    res.json({ message: 'Password changed successfully.' });
  } catch (error) {
    res.status(500).json({ message: 'Password change failed.', error: error.message });
  }
});

// Health check endpoint
app.get('/', (req, res) => {
  res.send('Hello testing');
});

// Catch-all to serve frontend
app.use((req, res, next) => {
  const isApi = req.url.startsWith('/run-status') ||
                req.url.startsWith('/list-notebooks') ||
                req.url.startsWith('/run-notebook') ||
                req.url.startsWith('/auth') ||
                req.url.startsWith('/dashboard') ||
                req.url.startsWith('/organizations') ||
                req.url.startsWith('/users');

  const isAsset = req.url.includes('.');
  if (!isApi && !isAsset) {
    res.sendFile(path.join(process.cwd(), 'dist/index.html'));
  } else {
    next();
  }
});

// Start server
app.listen(PORT, () => {
  console.log(`Backend Server is running ..`);
});
