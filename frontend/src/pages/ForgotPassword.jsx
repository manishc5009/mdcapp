import { useState } from 'react';
import { Link } from 'react-router-dom';
import { ToastContainer, toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';

function ForgotPassword() {
  const [email, setEmail] = useState('');
  const [error, setError] = useState('');

  const validate = () => {
    if (!email.trim()) {
      return 'Email is required';
    } else if (!/^[\w-.]+@([\w-]+\.)+[\w-]{2,4}$/.test(email)) {
      return 'Email is invalid';
    }
    return '';
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    const validationError = validate();
    setError(validationError);

    if (!validationError) {
      // Simulate API call
      toast.success('Password reset link sent to your email!', { autoClose: 3000 });
      setEmail('');
    } else {
      toast.error(validationError, { autoClose: 3000 });
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-white dark:bg-dark">
      <ToastContainer />
      <div className="w-full max-w-md bg-white dark:bg-dark-lighter rounded-lg shadow-lg p-8">
        <h2 className="text-2xl font-semibold text-gray-900 dark:text-white mb-4 text-center">Forgot Password</h2>
        <p className="text-gray-600 dark:text-gray-400 mb-6 text-center">
          Enter your email address and we’ll send you a link to reset your password.
        </p>
        <form onSubmit={handleSubmit} noValidate>
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-900 dark:text-gray-200 mb-2">Email</label>
            <input
              type="email"
              name="email"
              value={email}
              onChange={e => setEmail(e.target.value)}
              placeholder="your@email.com"
              className={`w-full px-4 py-2 rounded-lg border ${error ? 'border-red-500' : 'border-gray-300 dark:border-dark-lighter'} bg-white dark:bg-dark text-gray-900 dark:text-gray-200 placeholder-gray-500 dark:placeholder-gray-400 focus:ring-2 focus:ring-primary focus:border-transparent`}
            />
            {error && <p className="text-red-500 text-xs mt-1">{error}</p>}
          </div>
          <button
            type="submit"
            className="w-full bg-primary text-white py-2 px-4 rounded-lg hover:bg-primary/90 transition-colors"
          >
            Send Reset Link
          </button>
        </form>
        <div className="text-center mt-6">
          <Link to="/" className="text-primary hover:text-primary/90 text-sm">
            Back to Login
          </Link>
        </div>
      </div>
    </div>
  );
}

export default ForgotPassword;