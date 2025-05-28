import React, { useState, useEffect } from 'react';
import { useLocation } from 'react-router-dom';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faFileExcel, faArrowRight, faArrowLeft, faDownload, faSpinner } from '@fortawesome/free-solid-svg-icons';
import * as XLSX from 'xlsx';
import { v4 as uuidv4 } from 'uuid';

import { toast, ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import ParsedPreview from '../components/ParsedPreview';

const steps = [
	'upload',
	'viewData',
	'summary',
	'uploadAzure',
	'executeNotebook',
	'outputData',
];

const ProgressStep = ({ number, label, status }) => {
	const baseClass = 'progress-step flex-1 relative flex flex-col items-center';
	const afterClass = status === 'completed' || status === 'active' ? 'bg-blue-500' : 'bg-gray-300';
	// Ensure bg-blue-500 is applied to the upload circle when active
	const stepNumberClass = `step-number flex items-center justify-center w-8 h-8 rounded-full border-2 mb-1 z-[999] ${
		status === 'completed' ? 'bg-blue-500 border-blue-500 text-white' : status === 'active' ? 'bg-blue-500 border-blue-500 text-white z-[999]' : 'bg-white border-gray-300 text-gray-500'
	}`;

	return (
		<div className={baseClass}>
			<div className={stepNumberClass}>{number}</div>
			<span className={`text-sm font-medium ${status === 'active' ? 'text-blue-500' : 'text-gray-500'}`}>{label}</span>
					{number !== steps.length && (
						<>
							<div
								className={`absolute top-4 left-1/2 w-full h-0.5 -translate-x-1 z-0 ${afterClass}`}
								style={{ height: '2px' }}
							/>
						</>
					)}
		</div>
	);
};

const SummaryBlock = ({ title, value, singleLine }) => (
	<div className={`bg-gray-50 p-4 rounded-md shadow-sm flex ${singleLine ? 'flex-row space-x-2' : 'flex-col'} items-center justify-center w-50`}>
		<span className="text-sm text-gray-500">{title}:</span>
		<span className="text-lg font-semibold text-gray-800 mt-1">{value}</span>
	</div>
);

const UploadSection = () => {
	const location = useLocation();

	const [currentStep, setCurrentStep] = useState(0);
	const [file, setFile] = useState(null);
	const [dragOver, setDragOver] = useState(false);
	const [workbook, setWorkbook] = useState(null);
	const [sheetNames, setSheetNames] = useState([]);
	const [selectedSheet, setSelectedSheet] = useState('');
	const [sheetData, setSheetData] = useState([]);

	// Add state for select dropdown
	const [selectedOption, setSelectedOption] = useState(''); // <-- Start with empty string
	const [selectedDataId, setSelectedDataId] = useState('');
	const [dropdownError, setDropdownError] = useState('');
	const [parsedPage, setParsedPage] = useState(1);
	const rowsPerPage = 20;

	const [uploading, setUploading] = useState(false);
	const [uploadCompleted, setUploadCompleted] = useState(false);
	const [progress, setProgress] = useState(0);
	const [executeProgress, setExecuteProgress] = useState(0);
	const [runStatusLoading, setRunStatusLoading] = useState(false);
	const [pollIntervalId, setPollIntervalId] = useState(null);
	const [azureParsingLoading, setAzureParsingLoading] = useState(false)

	const [parsedData, setParsedData] = useState([]);

	// New state to hold task data for persistence
	const [taskData, setTaskData] = useState(() => {
		// Load from localStorage if available
		const saved = localStorage.getItem('uploadTaskData');
		return saved ? JSON.parse(saved) : { taskId: uuidv4() };
	});

	// New state to hold logged-in user info
	const [loggedInUser, setLoggedInUser] = useState({});

	// Load logged-in user info from localStorage on mount
	useEffect(() => {
		const user = localStorage.getItem('mdc_auth_user');
		if (user) {
			try {
				setLoggedInUser(JSON.parse(user));
			} catch (e) {
				console.error('Failed to parse loggedInUser from localStorage', e);
				setLoggedInUser({});
			}
		}
	}, []);

	// Save taskData to localStorage whenever it changes
	useEffect(() => {
		localStorage.setItem('uploadTaskData', JSON.stringify(taskData));
	}, [taskData]);

	// Update taskData with fileSize and total_rows when on step 2 and file or sheetData changes
	useEffect(() => {
		if (currentStep === 2 && file && sheetData.length) {
			updateTaskData({
				fileSize: file ? (file.size > 1024 * 1024 ? (file.size / (1024 * 1024)).toFixed(2) + ' MB' : (file.size / 1024).toFixed(2) + ' KB') : 'N/A',
				total_rows: sheetData.length,
			});
		}
	}, [currentStep, file, sheetData]);

	// On mount, restore state from taskData or resumeTask from navigation state
	useEffect(() => {
		if (location.state && location.state.resumeTask) {
			const resumeTask = location.state.resumeTask;
			// Initialize state from resumeTask
			setTaskData(resumeTask);
			if (resumeTask.selectedOption) setSelectedOption(resumeTask.selectedOption);
			if (resumeTask.selectedDataId) setSelectedDataId(resumeTask.selectedDataId);
			if (resumeTask.fileName) {
				// file object cannot be saved in localStorage, so just restore fileName for display
			}
			if (resumeTask.uploadCompleted !== undefined) setUploadCompleted(resumeTask.uploadCompleted);
			if (resumeTask.currentStep !== undefined) setCurrentStep(resumeTask.currentStep);
			// Clear location state to prevent repeated resume
			window.history.replaceState({}, document.title);
		} else if (taskData) {
			if (taskData.selectedOption) setSelectedOption(taskData.selectedOption);
			if (taskData.selectedDataId) setSelectedDataId(taskData.selectedDataId);
			if (taskData.fileName) {
				// file object cannot be saved in localStorage, so just restore fileName for display
			}
			if (taskData.uploadCompleted !== undefined) setUploadCompleted(taskData.uploadCompleted);
			if (taskData.currentStep !== undefined) setCurrentStep(taskData.currentStep);
		}
	}, []);

	// Helper to update taskData and save
	const updateTaskData = (newData) => {
		setTaskData(prev => ({ ...prev, ...newData }));
	};

	const nextStep = () => {
		if (currentStep < steps.length - 1) {
			const newStep = currentStep + 1;
			setCurrentStep(newStep);
			updateTaskData({ currentStep: newStep });
		}
	};

	const prevStep = () => {
		if (currentStep > 0) {
			const newStep = currentStep - 1;
			setCurrentStep(newStep);
			updateTaskData({ currentStep: newStep });
		}
	};

	const onFileChange = (e) => {
		if (e.target.files.length > 0) {
			const selectedFile = e.target.files[0];
			setFile(selectedFile);
			updateTaskData({ fileName: selectedFile.name });
			readExcelFile(selectedFile);
		}
	};

	const onDrop = (e) => {
		e.preventDefault();
		setDragOver(false);
		if (e.dataTransfer.files.length > 0) {
			const droppedFile = e.dataTransfer.files[0];
			setFile(droppedFile);
			updateTaskData({ fileName: droppedFile.name });
			readExcelFile(droppedFile);
		}
	};

	const onDragOver = (e) => {
		e.preventDefault();
		setDragOver(true);
	};

	const onDragLeave = (e) => {
		e.preventDefault();
		setDragOver(false);
	};

	const handleSheetChange = (e) => {
			const sheetName = e.target.value;
			setSelectedSheet(sheetName);
			if (workbook) {
				const ws = workbook.Sheets[sheetName];
				const jsonData = XLSX.utils.sheet_to_json(ws, { defval: '' });
				setSheetData(jsonData);
			}
		};

	const readExcelFile = (file) => {
		const reader = new FileReader();
		reader.onload = (evt) => {
			const data = new Uint8Array(evt.target.result);
			const wb = XLSX.read(data, { type: 'array' });
			setWorkbook(wb);
			setSheetNames(wb.SheetNames);
			setSelectedSheet(wb.SheetNames[0]);
			const ws = wb.Sheets[wb.SheetNames[0]];
			const jsonData = XLSX.utils.sheet_to_json(ws, { defval: '' });
			setSheetData(jsonData);
		};
		reader.readAsArrayBuffer(file);
	};

	const handleExecuteClick = async () => {
		const validNotebooks = [
			'Google', 'Meta', 'Neilsen', 'Amazon', 'Walmart', 'Circana',
			'LinkedIn', 'Tiktok', 'Snapchat', 'Youtube', 'Twitter'
		];
		if (!validNotebooks.includes(selectedDataId)) {
			toast.error(`Notebook for source "${selectedDataId}" not found.`);
			return;
		}
		setRunStatusLoading(true);
		setExecuteProgress(0);

		try {
			const apiurl = import.meta.env.VITE_API_URL;
			const response = await fetch(`${apiurl}/api/databricks/run-notebook`, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					fileName: file ? file.name : taskData.fileName,
					source: selectedDataId,
				}),
			});

			const result = await response.json();
			if (response.ok && result.run_id) {
				const pollInterval = setInterval(async () => {
					try {
						const statusResponse = await fetch(`${apiurl}/api/databricks/run-status/${result.run_id}`);
						const statusResult = await statusResponse.json();
						if (statusResponse.ok) {
							if (statusResult.status === 'completed') {
								clearInterval(pollInterval);
								setExecuteProgress(100);
								setRunStatusLoading(false);
								const newStep = currentStep + 1;
								setCurrentStep(newStep);
								updateTaskData({ currentStep: newStep });
								toast.success('Notebook run completed successfully!');
							} else if (statusResult.status === 'failed') {
								clearInterval(pollInterval);
								setExecuteProgress(0);
								setRunStatusLoading(false);
								toast.error('Notebook run failed.');
							} else if (typeof statusResult.progress === 'number') {
								setExecuteProgress(statusResult.progress);
							}
						} else {
							console.error('Failed to get run status:', statusResult);
						}
					} catch (pollError) {
						console.error('Error polling run status:', pollError);
					}
				}, 3000);

				setPollIntervalId(pollInterval);
			} else {
				console.error('Notebook execution failed:', result);
				toast.error('Notebook execution failed.');
				setRunStatusLoading(false);
			}
		} catch (error) {
			console.error('Error executing notebook:', error);
			toast.error('An error occurred while executing the notebook.');
			setRunStatusLoading(false);
		}
	};

	useEffect(() => {
		return () => {
			if (pollIntervalId) {
				clearInterval(pollIntervalId);
			}
		};
	}, [pollIntervalId]);

	useEffect(() => {
		if (currentStep !== 4) {
			setExecuteProgress(0);
			if (pollIntervalId) {
				clearInterval(pollIntervalId);
				setPollIntervalId(null);
			}
		}
	}, [currentStep]);

	useEffect(() => {
		const fetchProcessedData = async () => {
			if (currentStep === 5) { // or whatever step is for processed data
				setAzureParsingLoading(true)
				setParsedData([])
				setFile(null)
				try {
					const accountName = import.meta.env.VITE_AZURE_ACCOUNT_NAME
					const containerName = import.meta.env.VITE_AZURE_CONTAINER_NAME
					const sasToken = import.meta.env.VITE_AZURE_SAS_TOKEN
					const outputPath = import.meta.env.VITE_AZURE_OUTPUT_PATH || 'output/MMM_Data_Cube/MMM_Data_Cube'
					if (!accountName || !containerName || !sasToken) {
						throw new Error('Azure storage configuration missing')
					}
					const url = `https://${accountName}.blob.core.windows.net/${containerName}/${outputPath}.csv${sasToken}`

					const response = await fetch(url)
					if (!response.ok) {
						throw new Error(`Failed to fetch processed data: ${response.statusText}`)
					}
					const csvText = await response.text()
					const workbook = XLSX.read(csvText, { type: 'string' })
					const sheetName = workbook.SheetNames[0]
					const worksheet = workbook.Sheets[sheetName]
					const jsonData = XLSX.utils.sheet_to_json(worksheet)
					setParsedData(jsonData)
				} catch (error) {
					console.error('Error fetching processed data:', error)
					toast.error('Failed to load processed data.')
				} finally {
					setAzureParsingLoading(false)
				}
			}
		}
		fetchProcessedData();
	}, [currentStep])


	const uploadFileToAzure = async (file) => {
		setUploading(true);
		setUploadCompleted(false);
		setProgress(0);

		try {
			const containerName = import.meta.env.VITE_AZURE_CONTAINER_NAME;
			const folderPath = import.meta.env.VITE_AZURE_UPLOAD_PATH;
			const sasToken = import.meta.env.VITE_AZURE_SAS_TOKEN;
			const accountName = import.meta.env.VITE_AZURE_ACCOUNT_NAME;

			if (!sasToken || !accountName || !containerName || !folderPath) {
				throw new Error("Azure config missing");
			}

			const blobName = `${folderPath}/${selectedOption}/${file.name}`;
			const url = `https://${accountName}.blob.core.windows.net/${containerName}/${blobName}${sasToken}`;

			const xhr = new XMLHttpRequest();
			xhr.open("PUT", url, true);
			xhr.setRequestHeader("x-ms-blob-type", "BlockBlob");
			xhr.setRequestHeader("Content-Type", file.type || "application/octet-stream");

			xhr.upload.onprogress = (e) => {
				if (e.lengthComputable) {
					const progress = Math.round((e.loaded / e.total) * 100);
					setProgress(progress);
				}
			};

			xhr.onload = () => {
				if (xhr.status >= 200 && xhr.status < 300) {
					setUploadCompleted(true);
					updateTaskData({ uploadCompleted: true });
					nextStep(); // Move to the next step
				} else {
					alert(`Upload failed: ${xhr.statusText}`);
				}
				setUploading(false);
			};

			xhr.onerror = () => {
				alert("Network error during upload");
				setUploading(false);
			};
			xhr.send(file);
		} catch (error) {
			console.error("Upload to Azure failed:", error);
			alert(error.message || "Upload failed");
			setUploading(false);
		}
	};

	const handleDownload = () => {
		const worksheet = XLSX.utils.json_to_sheet(parsedData);
		const workbook = XLSX.utils.book_new();
		XLSX.utils.book_append_sheet(workbook, worksheet, "ProcessedData");
		XLSX.writeFile(workbook, "final_output_processed_data.xlsx");
	};

	const handleNextFromUpload = () => {
		if (!selectedOption) {
			setDropdownError('Please select a data source.');
			return;
		}
		setDropdownError('');
		updateTaskData({ selectedOption, selectedDataId, currentStep: currentStep + 1 });
		nextStep();
	};

	// Automatically call /register-notebook API on last step
	useEffect(() => {
		const registerNotebook = async () => {
			if (currentStep === steps.length - 1 && Object.keys(loggedInUser).length > 0) {
				try {
					// console.log('LoggedInUser object:', loggedInUser);
					const apiurl = import.meta.env.VITE_API_URL;
					const payload = {
						taskId: taskData.taskId,
						fileName: taskData.fileName,
						filesize: taskData.fileSize,
						total_rows: taskData.total_rows,
						selectedOption: taskData.selectedOption,
						selectedDataId: taskData.selectedDataId,
						currentStep: currentStep,
						uploadCompleted: taskData.uploadCompleted || false,
						...loggedInUser,
					};
					console.log('Register notebook payload:', payload);
					const response = await fetch(`${apiurl}/api/databricks/register-notebook`, {
						method: 'POST',
						headers: { 'Content-Type': 'application/json' },
						body: JSON.stringify(payload),
					});
					if (!response.ok) {
						throw new Error('Failed to register notebook');
					}
					toast.success('Notebook registered successfully.');
					localStorage.removeItem('uploadTaskData');
					setTaskData({ taskId: uuidv4() });
				} catch (error) {
					console.error('Error registering notebook:', error);
					toast.error('Failed to register notebook.');
				}
			}
		};
		registerNotebook();
	}, [currentStep, loggedInUser]);

	return (
		<div className="bg-white rounded-xl shadow-lg overflow-hidden border border-gray-200">
			<ToastContainer />
			{/* Progress Steps */}
			<div className="p-6 border-b border-gray-200 relative">
				<h2 className="text-xl font-semibold text-gray-800 mb-4">Process Excel File</h2>
				{currentStep >= 4  && (
<button
  className="absolute top-4 right-4 px-4 py-2 bg-yellow-400 text-black rounded-md hover:bg-yellow-500 transition"
  onClick={async () => {
    try {
      const apiurl = import.meta.env.VITE_API_URL;
      const savedTaskData = localStorage.getItem('uploadTaskData');
      if (!savedTaskData) {
        toast.error('No task data found to pause.');
        return;
      }
      const taskData = JSON.parse(savedTaskData);
      const loggedInUser = JSON.parse(localStorage.getItem('mdc_auth_user') || '{}');
      const payload = {
        taskId: taskData.taskId,
        fileName: taskData.fileName,
        filesize: taskData.fileSize,
        total_rows: taskData.total_rows,
        selectedOption: taskData.selectedOption,
        selectedDataId: taskData.selectedDataId,
        currentStep: taskData.currentStep,
        uploadCompleted: false,
        ...loggedInUser,
        status: 0 // paused status
      };
      const response = await fetch(`${apiurl}/api/databricks/register-notebook`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (!response.ok) {
        throw new Error('Failed to pause and save task.');
      }
      toast.success('Task paused and saved successfully.');
      localStorage.removeItem('uploadTaskData');
      // Reset all relevant state variables to initial values to simulate reload
      setTaskData({ taskId: uuidv4() });
      setFile(null);
      setDragOver(false);
      setWorkbook(null);
      setSheetNames([]);
      setSelectedSheet('');
      setSheetData([]);
      setSelectedOption('');
      setSelectedDataId('');
      setDropdownError('');
      setParsedPage(1);
      setUploading(false);
      setUploadCompleted(false);
      setProgress(0);
      setExecuteProgress(0);
      setRunStatusLoading(false);
      if (pollIntervalId) {
        clearInterval(pollIntervalId);
        setPollIntervalId(null);
      }
      setAzureParsingLoading(false);
      setParsedData([]);
      setCurrentStep(0);
    } catch (error) {
      console.error('Error pausing task:', error);
      toast.error('Failed to pause and save task.');
    }
  }}
  title="Pause and save task"
>
  Pause
</button>
				)}
				<div className="mb-8 flex justify-between relative">
					{steps.map((step, index) => {
						let status = 'pending';
						if (index < currentStep) status = 'completed';
						else if (index === currentStep) status = 'active';
						return (
							<ProgressStep
								key={step}
								number={index + 1}
								label={step.charAt(0).toUpperCase() + step.slice(1)}
								status={status}
							/>
						);
					})}
				</div>
			</div>

			{/* Step Content */}
			<div className="p-6">
				{currentStep === 0 && (
					<>
						{/* Buttons on top */}
						<div className="flex justify-end mb-6">
							<button
								id="next-from-upload"
								className={`px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition ${
									!file || !selectedOption ? 'opacity-50 cursor-not-allowed' : ''
								}`}
								disabled={!file || !selectedOption}
								onClick={handleNextFromUpload}
							>
								Next <FontAwesomeIcon icon={faArrowRight} className="ml-2" />
							</button>
						</div>
						{/* Data Source Dropdown */}
						<div className="mb-6 flex flex-col w-full">
							<label htmlFor="selectOption" className="select-label mb-2 font-medium text-gray-700">
								Choose your data source:
							</label>
							<select
								id="selectOption"
								value={selectedOption}
								onChange={(e) => {
									const selectedIndex = e.target.selectedIndex;
									const dataId = e.target.options[selectedIndex].getAttribute('data-id');
									setSelectedOption(e.target.value);
									setSelectedDataId(dataId);
									setDropdownError('');
								}}
								style={{ width: '100%' }}
								className={`select-input border border-gray-300 rounded-md px-3 py-2 w-full block ${dropdownError ? 'border-red-500' : ''}`}
							>
								<option value="" disabled>-- Select --</option>
								<option value="google_ads" data-id="Google">Google Ads</option>
								<option value="meta" data-id="Meta">Meta</option>
								<option value="neilsen" data-id="Neilsen">Neilsen</option>
								<option value="Amazon" data-id="Amazon">Amazon</option>
								<option value="Walmart" data-id="Walmart">Walmart</option>
								<option value="Circana" data-id="Circana">Circana</option>
								<option value="LinkedIn" data-id="LinkedIn">LinkedIn</option>
								<option value="Tiktok" data-id="Tiktok">Tiktok</option>
								<option value="Snapchat" data-id="Snapchat">Snapchat</option>
								<option value="Youtube" data-id="Youtube">Youtube</option>
								<option value="Twitter" data-id="Twitter">Twitter</option>
							</select>
							{dropdownError && (
								<span className="text-red-500 text-sm mt-1">{dropdownError}</span>
							)}
						</div>
						{/* File upload area */}
						<div
							id="drop-area"
							className={`file-upload rounded-lg p-8 text-center mb-6 border-2 border-dashed ${dragOver ? 'border-blue-500 bg-blue-50' : 'border-gray-300'}`}
							onDrop={onDrop}
							onDragOver={onDragOver}
							onDragLeave={onDragLeave}
						>
							<FontAwesomeIcon icon={faFileExcel} className="text-4xl text-blue-500 mb-4" />
							<h3 className="text-lg font-medium text-gray-800 mb-2">Drag & Drop your Excel file here</h3>
							<p className="text-gray-500 mb-4">or</p>
							<input key={file ? file.name : 'file-input'} type="file" id="file-input" accept=".xlsx,.xls,.csv" className="hidden" onChange={onFileChange} />
							<label htmlFor="file-input" className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition cursor-pointer">
								Browse Files
							</label>
							{file && (
								<div id="file-info" className="mt-4">
									<p className="text-sm text-gray-600">
										Selected file: <span className="font-medium">{file.name}</span>
									</p>
									<p className="text-xs text-gray-500">{file.size > 1024 * 1024 ? (file.size / (1024 * 1024)).toFixed(2) + ' MB' : (file.size / 1024).toFixed(2) + ' KB'}</p>
								</div>
							)}
						</div>
					</>
				)}

				{currentStep === 1 && (
					<div className="w-full">
						{/* Buttons on top */}
						<div className="flex justify-between mb-6">
							<button className="px-6 py-2 bg-gray-200 text-gray-700 rounded-md hover:bg-gray-300 transition" onClick={prevStep}>
								<FontAwesomeIcon icon={faArrowLeft} className="mr-2" /> Back
							</button>
							<button className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition" onClick={nextStep}>
								Next <FontAwesomeIcon icon={faArrowRight} className="ml-2" />
							</button>
						</div>
						{/* Sheet selector dropdown */}
						<div className="mb-4">
							<label htmlFor="sheet-select" className="block mb-2 font-medium text-gray-700">Select Sheet:</label>
							<select
								id="sheet-select"
								value={selectedSheet}
								onChange={handleSheetChange}
								className="border border-gray-300 rounded-md px-3 py-2 w-full"
							>
								{sheetNames.map((name) => (
									<option key={name} value={name}>{name}</option>
								))}
							</select>
						</div>
						{/* Main content */}
						<h3 className="text-lg font-medium text-gray-800 mb-4">Parsed Data Preview</h3>
						<div className="overflow-x-auto w-full border border-gray-300 rounded-md mb-4" style={{ maxWidth: '77vw' }}>
							<table className="min-w-max divide-y divide-gray-200 text-sm">
								<thead className="bg-gray-50 sticky top-0">
									<tr>
										{sheetData.length > 0 &&
											Object.keys(sheetData[0]).map((col) => (
												<th
													key={col}
													className="px-4 py-2 text-left font-medium text-gray-700 break-words truncate"
													style={{ minWidth: 120, maxWidth: 200, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}
													title={col}
												>
													{col}
												</th>
											))}
									</tr>
								</thead>
								<tbody className="divide-y divide-gray-200">
									{sheetData
										.slice((parsedPage - 1) * rowsPerPage, parsedPage * rowsPerPage)
										.map((row, idx) => (
											<tr key={idx} className={idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
										{Object.values(row).map((val, i) => (
											<td
												key={i}
												className="px-4 py-2 whitespace-nowrap break-words truncate"
												style={{ minWidth: 120, maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis' }}
												title={val}
											>
												{val}
											</td>
										))}
									</tr>
										))}
								</tbody>
							</table>
						</div>
						{/* Pagination controls */}
						{sheetData.length > rowsPerPage && (
							<div className="flex justify-between items-center mt-2">
								<button
									className="px-3 py-1 bg-gray-200 rounded disabled:opacity-50"
									onClick={() => setParsedPage((p) => Math.max(1, p - 1))}
									disabled={parsedPage === 1}
								>
									Previous
								</button>
								<span>
									Page {parsedPage} of {Math.ceil(sheetData.length / rowsPerPage)}
								</span>
								<button
									className="px-3 py-1 bg-gray-200 rounded disabled:opacity-50"
									onClick={() => setParsedPage((p) => Math.min(Math.ceil(sheetData.length / rowsPerPage), p + 1))}
									disabled={parsedPage === Math.ceil(sheetData.length / rowsPerPage)}
								>
									Next
								</button>
							</div>
						)}
					</div>
				)}

				{currentStep === 2 && (
					<div>
						{/* Buttons on top */}
						<div className="flex justify-between mb-6">
							<button className="px-6 py-2 bg-gray-200 text-gray-700 rounded-md hover:bg-gray-300 transition" onClick={prevStep}>
								<FontAwesomeIcon icon={faArrowLeft} className="mr-2" /> Back
							</button>
							<button className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition" onClick={nextStep}>
								Next <FontAwesomeIcon icon={faArrowRight} className="ml-2" />
							</button>
						</div>
						{/* Main content */}
						<h3 className="text-lg font-medium text-gray-800 mb-6">Uploaded File Summary</h3>
						<div className="mb-6">
							<SummaryBlock title="File Name" value={file ? file.name : 'N/A'} singleLine={false} />
						</div>
						<div className="grid grid-cols-2 gap-6">
							<SummaryBlock title="File Size" value={file ? (file.size > 1024 * 1024 ? (file.size / (1024 * 1024)).toFixed(2) + ' MB' : (file.size / 1024).toFixed(2) + ' KB') : 'N/A'} />
							<SummaryBlock title="Number of Rows" value={sheetData.length} />
							<SummaryBlock title="Number of Columns" value={sheetData.length > 0 ? Object.keys(sheetData[0]).length : 0} />
							<SummaryBlock title="Selected Sheet" value={selectedSheet} />
						</div>
					</div>
				)}

				{currentStep === 3 && (
					<div>
						{/* Buttons on top */}
						<div className="flex justify-between mb-6">
							<button
								className="px-6 py-2 bg-gray-200 text-gray-700 rounded-md hover:bg-gray-300 transition"
								onClick={prevStep}
							>
								<FontAwesomeIcon icon={faArrowLeft} className="mr-2" /> Back
							</button>
							{uploadCompleted ? (
								<button
									className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition"
									onClick={nextStep}
								>
									Next <FontAwesomeIcon icon={faArrowRight} className="ml-2" />
								</button>
							) : (
								<button
									className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition"
									onClick={() => uploadFileToAzure(file)}
									disabled={uploading || !file}
								>
									{uploading ? `Uploading... ${progress}%` : "Upload to Azure"}
									{uploading && <FontAwesomeIcon icon={faSpinner} spin className="ml-2" />}
								</button>
							)}
						</div>
						{/* Main content */}
						<h3 className="text-lg font-medium text-gray-800 mb-4">Uploading to Azure Blob Storage</h3>
						<div className="w-full bg-gray-200 rounded-full h-6 mb-4 overflow-hidden">
							<div
								className="bg-blue-600 h-6 rounded-full transition-all duration-300"
								style={{ width: `${progress}%`, minWidth: '2rem' }}
							/>
						</div>
						<p className="text-right text-sm font-medium text-blue-600">{progress}% uploaded</p>
					</div>
				)}

				{currentStep === 4 && (
					<div>
						{/* Buttons on top */}
						<div className="flex justify-between mb-6">
							<button className="px-6 py-2 bg-gray-200 text-gray-700 rounded-md hover:bg-gray-300 transition" onClick={prevStep}>
								<FontAwesomeIcon icon={faArrowLeft} className="mr-2" /> Back
							</button>
							<button
								className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition"
								onClick={handleExecuteClick}
								disabled={runStatusLoading}
								style={{ position: 'relative' }}
							>
								{runStatusLoading ? `Executing... ${executeProgress}%` : 'Execute'}
							</button>
						</div>
						{/* Progress bar and percentage BELOW the buttons */}
						{runStatusLoading && (
							<div className="mt-4">
								<div className="w-full bg-gray-200 rounded-full h-6 mb-4 overflow-hidden">
									<div
										className="bg-blue-600 h-6 rounded-full transition-all duration-300"
										style={{ width: `${executeProgress}%`, minWidth: '2rem' }}
									/>
								</div>
								<p className="text-right text-sm font-medium text-blue-600">{executeProgress}% completed</p>
							</div>
						)}
						{/* Main content */}
						<h3 className="text-lg font-medium text-gray-800 mb-4">Executing Notebook</h3>
					</div>
				)}

				{currentStep === 5 && (
					<div>
						{/* Buttons on top */}
						<div className="flex space-x-4 mb-6">
							<button
								className="px-6 py-2 bg-gray-200 text-gray-700 rounded-md hover:bg-gray-300 transition"
								onClick={prevStep}
							>
								<FontAwesomeIcon icon={faArrowLeft} className="mr-2" /> Back
							</button>
							<button
								className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition"
								onClick={handleDownload} // <-- Use your custom download function here
							>
								<FontAwesomeIcon icon={faDownload} className="mr-2" />
								Download Output
							</button>
							<button
								className="px-6 py-2 bg-gray-200 text-gray-700 rounded-md hover:bg-gray-300 transition"
								onClick={() => alert('View output clicked')}
							>
								View Output
							</button>
						</div>
						{/* Main content */}
						<h3 className="text-lg font-medium text-gray-800 mb-4">Output Data</h3>
						{azureParsingLoading ? (
							<div className="spinner-wrapper" style={{ display: 'flex', justifyContent: 'center', marginTop: '2rem' }}>
								<div className="spinner-border" role="status" style={{
									width: '3rem',
									height: '3rem',
									border: '0.4em solid #f3f3f3',
									borderTop: '0.4em solid #2563eb',
									borderRadius: '50%',
									animation: 'spin 1s linear infinite',
								}} />
							</div>
						) : (
							<>
								{/* Table or ParsedPreview */}
								<ParsedPreview
								  data={parsedData}
								  title="Processed Data Preview:"
								  subtitle="Below is the Processed data ready for review and interpretation."
								/>
								<div className="text-center mt-8 download_btn">
									<button onClick={handleDownload} className="download-button px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition">
										<FontAwesomeIcon icon={faDownload} style={{ marginRight: '0.5rem' }} />
										Download Processed Data
									</button>
								</div>
							</>
						)}
					</div>
				)}
			</div>
		</div>
	);
};


export default UploadSection;
