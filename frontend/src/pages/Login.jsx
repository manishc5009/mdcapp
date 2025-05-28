import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import loginIllustration from '../assets/images/v2-login-dark.png';
import { ToastContainer, toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { setToken, setUser } from '../utils/auth';

const apiurl = import.meta.env.VITE_API_URL;

function Login() {
  const [form, setForm] = useState({
    email: '',
    password: '',
    remember: false,
  });
  const [errors, setErrors] = useState({});
  const navigate = useNavigate();

  const validate = () => {
    const newErrors = {};
    if (!form.email.trim()) {
      newErrors.email = 'Email is required';
    } else if (!/^[\w-.]+@([\w-]+\.)+[\w-]{2,4}$/.test(form.email)) {
      newErrors.email = 'Email is invalid';
    }
    if (!form.password) {
      newErrors.password = 'Password is required';
    }
    return newErrors;
  };

  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    setForm((prev) => ({
      ...prev,
      [name]: type === 'checkbox' ? checked : value,
    }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    const validationErrors = validate();
    setErrors(validationErrors);
    if (Object.keys(validationErrors).length === 0) {
      try {
        const response = await fetch(`${apiurl}/api/auth/login`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            email: form.email,
            password: form.password,
          }),
        });

        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(errorData.message || 'Login failed');
        }

        const data = await response.json();
        if (data.token) {
          setToken(data.token);
          setUser(data.user);
          toast.success('Login successful!', { autoClose: 2000 });
          setTimeout(() => {
            navigate('/dashboard');
          }, 2000);
        } else {
          throw new Error('Invalid login response');
        }
      } catch (error) {
        toast.error(error.message || 'Login failed', { autoClose: 3000 });
      }
    } else {
      toast.error('Please fix the errors in the form.', { autoClose: 3000 });
    }
  };

  return (
    <div className="min-h-screen flex bg-white dark:bg-dark">
      <ToastContainer />
      <div className="hidden lg:flex flex-1 relative bg-white dark:bg-dark items-center justify-center">
        {/* Light/dark mode background for illustration */}
        <div className="absolute inset-0 bg-auth-mask bg-no-repeat bg-cover opacity-60 dark:opacity-60"></div>
        <div className="absolute bottom-0 left-0 right-0 bg-auth-tree bg-no-repeat bg-bottom bg-contain h-2/3"></div>
        <div className="relative z-10 text-center max-w-lg">
          <img
            src={loginIllustration}
            alt="Login illustration"
            className="w-full max-w-md mx-auto"
          />
        </div>
      </div>
      <div className="flex-1 flex items-center justify-center p-8">
        <div className="w-full max-w-[400px] space-y-8 bg-white dark:bg-dark-lighter rounded-lg shadow-md p-8">
          <div className="text-center">
            <h4 className="text-2xl font-semibold text-gray-900 dark:text-white mb-1">
              Welcome to <br />Marketing Data Cloud! 👋
            </h4>
            <p className="text-gray-600 dark:text-gray-400">
              Please sign in to your account and start the adventure
            </p>
          </div>

          <form className="space-y-6" onSubmit={handleSubmit} noValidate>
            <div>
              <label className="block text-sm font-medium text-gray-900 dark:text-gray-200 mb-2">Email</label>
              <input
                type="email"
                name="email"
                value={form.email}
                onChange={handleChange}
                placeholder="admin@mdc.com"
                className={`w-full px-4 py-2 rounded-lg border ${errors.email ? 'border-red-500' : 'border-gray-300 dark:border-dark-lighter'} bg-white dark:bg-dark text-gray-900 dark:text-gray-200 placeholder-gray-500 dark:placeholder-gray-400 focus:ring-2 focus:ring-primary focus:border-transparent`}
              />
              {errors.email && <p className="text-red-500 text-xs mt-1">{errors.email}</p>}
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-900 dark:text-gray-200 mb-2">Password</label>
              <input
                type="password"
                name="password"
                value={form.password}
                onChange={handleChange}
                placeholder="············"
                className={`w-full px-4 py-2 rounded-lg border ${errors.password ? 'border-red-500' : 'border-gray-300 dark:border-dark-lighter'} bg-white dark:bg-dark text-gray-900 dark:text-gray-200 placeholder-gray-500 dark:placeholder-gray-400 focus:ring-2 focus:ring-primary focus:border-transparent`}
              />
              {errors.password && <p className="text-red-500 text-xs mt-1">{errors.password}</p>}
            </div>

            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <label className="flex items-center cursor-pointer">
                  <input
                    type="checkbox"
                    name="remember"
                    checked={form.remember}
                    onChange={handleChange}
                    className="h-4 w-4 text-primary rounded border-gray-300 dark:border-dark-lighter bg-white dark:bg-dark"
                  />
                  <span className="ml-2 text-sm text-gray-700 dark:text-gray-300">Remember me</span>
                </label>
              </div>
              <Link to="/forgot-password" className="text-sm text-primary hover:text-primary/90">
                Forgot Password?
              </Link>
            </div>

            <button
              type="submit"
              className="w-full bg-primary text-white py-2 px-4 rounded-lg hover:bg-primary/90 transition-colors"
            >
              Sign in
            </button>
          </form>

          <div className="text-center mt-6">
            <p className="text-gray-400 dark:text-gray-500">
              New on our platform?{' '}
              <Link to="/signup" className="text-primary hover:text-primary/90">Create an account</Link>
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Login;
