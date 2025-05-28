import { Link } from 'react-router-dom';

function Dashboard() {
  return (
    <div className="min-h-screen flex items-center justify-center bg-white dark:bg-dark">
      <div className="bg-white dark:bg-dark-lighter rounded-lg shadow-lg p-8 max-w-md w-full text-center">
        <h2 className="text-3xl font-bold text-gray-900 dark:text-white mb-4">
          Welcome to your Dashboard!
        </h2>
        <p className="text-gray-600 dark:text-gray-400 mb-6">
          You have successfully logged in.
        </p>
        <Link to="/" className="text-primary hover:text-primary/90 text-sm">
          Log out
        </Link>
      </div>
    </div>
  );
}

export default Dashboard;