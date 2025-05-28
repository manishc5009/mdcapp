import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import signupIllustration from '../assets/images/v2-register-dark.png';
import { ToastContainer, toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';

const apiurl = import.meta.env.VITE_API_URL;

function generateCaptcha() {
  const a = Math.floor(Math.random() * 10) + 1;
  const b = Math.floor(Math.random() * 10) + 1;
  return { question: `What is ${a} + ${b}?`, answer: (a + b).toString() };
}

const steps = [
  'Account Info',
  'Password',
  'Company Info',
  'Verification'
];

function Signup() {
  const [form, setForm] = useState({
    fullName: '',
    email: '',
    password: '',
    confirmPassword: '',
    company: '',
    phone: '',
    agreement: false,
    captcha: '',
  });
  const [errors, setErrors] = useState({});
  const [captcha, setCaptcha] = useState(generateCaptcha());
  const [step, setStep] = useState(0);
  const navigate = useNavigate();

  const validateStep = () => {
    const newErrors = {};
    if (step === 0) {
      if (!form.fullName.trim()) newErrors.fullName = 'Full Name is required';
      if (!form.email.trim()) {
        newErrors.email = 'Email is required';
      } else if (!/^[\w-.]+@([\w-]+\.)+[\w-]{2,4}$/.test(form.email)) {
        newErrors.email = 'Email is invalid';
      }
    }
    if (step === 1) {
      if (!form.password) {
        newErrors.password = 'Password is required';
      } else if (form.password.length < 6) {
        newErrors.password = 'Password must be at least 6 characters';
      }
      if (!form.confirmPassword) {
        newErrors.confirmPassword = 'Please confirm your password';
      } else if (form.password !== form.confirmPassword) {
        newErrors.confirmPassword = 'Passwords do not match';
      }
    }
    if (step === 2) {
      if (!form.company.trim()) newErrors.company = 'Company Name is required';
      if (form.phone && !/^\+?\d{7,15}$/.test(form.phone)) {
        newErrors.phone = 'Phone number is invalid';
      }
    }
    if (step === 3) {
      if (!form.captcha) {
        newErrors.captcha = 'Captcha is required';
      } else if (form.captcha !== captcha.answer) {
        newErrors.captcha = 'Captcha answer is incorrect';
      }
      if (!form.agreement) {
        newErrors.agreement = 'You must agree to the terms and privacy policy';
      }
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

  const handleNext = (e) => {
    e.preventDefault();
    const validationErrors = validateStep();
    setErrors(validationErrors);
    if (Object.keys(validationErrors).length === 0) {
      setStep((prev) => prev + 1);
    } else {
      toast.error('Please fix the errors in this step.', { autoClose: 2000 });
    }
  };

  const handlePrev = (e) => {
    e.preventDefault();
    setErrors({});
    setStep((prev) => prev - 1);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    const validationErrors = validateStep();
    setErrors(validationErrors);

    if (Object.keys(validationErrors).length === 0) {
      try {
        const response = await fetch(`${apiurl}/api/users/`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            fullName: form.fullName,
            email: form.email,
            password: form.password,
            company: form.company,
            phone: form.phone,
          }),
        });

        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(errorData.message || 'Registration failed');
        }

        toast.success('Registration successful! Redirecting to login...', { autoClose: 2000 });
        setTimeout(() => {
          navigate('/login');
        }, 2000);
      } catch (error) {
        toast.error(error.message || 'Registration failed', { autoClose: 3000 });
        setCaptcha(generateCaptcha());
        setForm((prev) => ({ ...prev, captcha: '' }));
      }
    } else {
      toast.error('Please fix the errors in this step.', { autoClose: 2000 });
      setCaptcha(generateCaptcha());
      setForm((prev) => ({ ...prev, captcha: '' }));
    }
  };

  return (
    <div className="min-h-screen flex bg-white dark:bg-dark">
      <ToastContainer />
      <div className="hidden lg:flex flex-1 relative bg-white dark:bg-dark items-center justify-center">
        <div className="absolute inset-0 bg-auth-mask bg-no-repeat bg-cover opacity-60 dark:opacity-60"></div>
        <div className="absolute bottom-0 left-0 right-0 bg-auth-tree bg-no-repeat bg-bottom bg-contain h-2/3"></div>
        <div className="relative z-10 text-center max-w-lg">
          <img
            src={signupIllustration}
            alt="Signup illustration"
            className="w-full max-w-md mx-auto"
          />
        </div>
      </div>
      <div className="flex-1 flex items-center justify-center p-8">
        <div className="max-w-md w-full mx-auto p-8 bg-white dark:bg-dark-lighter rounded-lg shadow-md">
          <h4 className="text-2xl font-semibold text-gray-900 dark:text-white mb-1">
            Create your account
          </h4>
          <p className="text-gray-600 dark:text-gray-400 mb-6">
            Please fill in the information below to register.
          </p>
          <div className="text-center">
            <p className="text-gray-400 dark:text-gray-500">
              Step {step + 1} of {steps.length}: {steps[step]}
            </p>
            <div className="flex justify-center gap-2 mt-2">
              {steps.map((s, i) => (
                <span
                  key={s}
                  className={`h-2 w-2 rounded-full transition-colors duration-200
                    ${i === step
                      ? 'bg-primary'
                      : 'bg-gray-300 dark:bg-gray-600'
                    }`}
                ></span>
              ))}
            </div>
          </div>

          <form className="space-y-6" onSubmit={step === steps.length - 1 ? handleSubmit : handleNext} noValidate>
            {step === 0 && (
              <>
                <div>
                  <label className="block text-sm font-medium text-gray-900 dark:text-gray-200 mb-2">Full Name</label>
                  <input
                    type="text"
                    name="fullName"
                    value={form.fullName}
                    onChange={handleChange}
                    placeholder="Your full name"
                    className={`w-full px-4 py-2 rounded-lg border ${errors.fullName ? 'border-red-500' : 'border-gray-300 dark:border-dark-lighter'} bg-white dark:bg-dark text-gray-900 dark:text-gray-200 placeholder-gray-500 dark:placeholder-gray-400 focus:ring-2 focus:ring-primary focus:border-transparent`}
                  />
                  {errors.fullName && <p className="text-red-500 text-xs mt-1">{errors.fullName}</p>}
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-900 dark:text-gray-200 mb-2">Email Address</label>
                  <input
                    type="email"
                    name="email"
                    value={form.email}
                    onChange={handleChange}
                    placeholder="your@email.com"
                    className={`w-full px-4 py-2 rounded-lg border ${errors.email ? 'border-red-500' : 'border-gray-300 dark:border-dark-lighter'} bg-white dark:bg-dark text-gray-900 dark:text-gray-200 placeholder-gray-500 dark:placeholder-gray-400 focus:ring-2 focus:ring-primary focus:border-transparent`}
                  />
                  {errors.email && <p className="text-red-500 text-xs mt-1">{errors.email}</p>}
                </div>
              </>
            )}

            {step === 1 && (
              <>
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
                <div>
                  <label className="block text-sm font-medium text-gray-900 dark:text-gray-200 mb-2">Confirm Password</label>
                  <input
                    type="password"
                    name="confirmPassword"
                    value={form.confirmPassword}
                    onChange={handleChange}
                    placeholder="············"
                    className={`w-full px-4 py-2 rounded-lg border ${errors.confirmPassword ? 'border-red-500' : 'border-gray-300 dark:border-dark-lighter'} bg-white dark:bg-dark text-gray-900 dark:text-gray-200 placeholder-gray-500 dark:placeholder-gray-400 focus:ring-2 focus:ring-primary focus:border-transparent`}
                  />
                  {errors.confirmPassword && <p className="text-red-500 text-xs mt-1">{errors.confirmPassword}</p>}
                </div>
              </>
            )}

            {step === 2 && (
              <>
                <div>
                  <label className="block text-sm font-medium text-gray-900 dark:text-gray-200 mb-2">Company Name</label>
                  <input
                    type="text"
                    name="company"
                    value={form.company}
                    onChange={handleChange}
                    placeholder="Your company"
                    className={`w-full px-4 py-2 rounded-lg border ${errors.company ? 'border-red-500' : 'border-gray-300 dark:border-dark-lighter'} bg-white dark:bg-dark text-gray-900 dark:text-gray-200 placeholder-gray-500 dark:placeholder-gray-400 focus:ring-2 focus:ring-primary focus:border-transparent`}
                  />
                  {errors.company && <p className="text-red-500 text-xs mt-1">{errors.company}</p>}
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-900 dark:text-gray-200 mb-2">
                    Phone Number <span className="text-gray-500 dark:text-gray-400 text-xs">(optional)</span>
                  </label>
                  <input
                    type="tel"
                    name="phone"
                    value={form.phone}
                    onChange={handleChange}
                    placeholder="+1234567890"
                    className={`w-full px-4 py-2 rounded-lg border ${errors.phone ? 'border-red-500' : 'border-gray-300 dark:border-dark-lighter'} bg-white dark:bg-dark text-gray-900 dark:text-gray-200 placeholder-gray-500 dark:placeholder-gray-400 focus:ring-2 focus:ring-primary focus:border-transparent`}
                  />
                  {errors.phone && <p className="text-red-500 text-xs mt-1">{errors.phone}</p>}
                </div>
              </>
            )}

            {step === 3 && (
              <>
                <div>
                  <label className="block text-sm font-medium text-gray-900 dark:text-gray-200 mb-2">Captcha</label>
                  <div className="flex items-center gap-2">
                    <span className="bg-gray-100 dark:bg-dark-lighter text-gray-900 dark:text-gray-200 px-3 py-2 rounded">{captcha.question}</span>
                    <input
                      type="text"
                      name="captcha"
                      value={form.captcha}
                      onChange={handleChange}
                      placeholder="Answer"
                      className={`w-24 px-2 py-2 rounded-lg border ${errors.captcha ? 'border-red-500' : 'border-gray-300 dark:border-dark-lighter'} bg-white dark:bg-dark text-gray-900 dark:text-gray-200 placeholder-gray-500 dark:placeholder-gray-400 focus:ring-2 focus:ring-primary focus:border-transparent`}
                    />
                  </div>
                  {errors.captcha && <p className="text-red-500 text-xs mt-1">{errors.captcha}</p>}
                </div>
                <div className="flex items-center mt-4">
                  <label className="flex items-center cursor-pointer">
                    <input
                      type="checkbox"
                      name="agreement"
                      checked={form.agreement}
                      onChange={handleChange}
                      className={`h-4 w-4 text-primary rounded border-gray-300 dark:border-dark-lighter bg-white dark:bg-dark ${errors.agreement ? 'border-red-500' : ''}`}
                    />
                    <span className="ml-2 text-sm text-gray-700 dark:text-gray-300">
                      I agree to the{' '}
                      <a href="#" className="text-primary hover:text-primary/90">Terms and Privacy Policy</a>
                    </span>
                  </label>
                </div>
                {errors.agreement && <p className="text-red-500 text-xs mt-1">{errors.agreement}</p>}
              </>
            )}

            <div className="flex justify-between mt-8">
              {step > 0 && (
                <button
                  type="button"
                  onClick={handlePrev}
                  className="bg-gray-600 dark:bg-gray-400 text-white py-2 px-4 rounded-lg hover:bg-gray-500 dark:hover:bg-gray-500 transition-colors focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary"
                >
                  Previous
                </button>
              )}
              <div className="flex-1"></div>
              {step < steps.length - 1 ? (
                <button
                  type="submit"
                  className="bg-primary text-white py-2 px-4 rounded-lg hover:bg-primary/90 transition-colors focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary"
                >
                  Next
                </button>
              ) : (
                <button
                  type="submit"
                  className="bg-primary text-white py-2 px-4 rounded-lg hover:bg-primary/90 transition-colors focus-visible:outline focus-visible:outline-2 focus-visible:outline-primary"
                >
                  Sign up
                </button>
              )}
            </div>
          </form>

          <div className="text-center mt-10">
            <p className="text-gray-400">
              Already have an account?{' '}
              <Link to="/" className="text-primary hover:text-primary/90">Sign in instead</Link>
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}

export default Signup;
