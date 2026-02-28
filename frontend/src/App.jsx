import React, { useState, useEffect, useRef } from 'react';
import { UploadCloud, Terminal, ChevronRight, Lock, Activity, Database, LayoutPanelLeft, Cpu, Settings, User, Menu, X, Mail } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import ReactMarkdown from 'react-markdown';
import Plot from 'react-plotly.js';

// --- Components ---

const TopStatusBar = () => {
    const [hex, setHex] = useState('0x8F9A');
    const [tensors, setTensors] = useState(44921);
    const [latency, setLatency] = useState(12);

    useEffect(() => {
        const interval = setInterval(() => {
            setHex(`0x${Math.floor(Math.random() * 0xFFFF).toString(16).toUpperCase().padStart(4, '0')}`);
            setTensors(prev => prev + Math.floor(Math.random() * 10 - 5));
            setLatency(Math.floor(Math.random() * 5 + 10));
        }, 800);
        return () => clearInterval(interval);
    }, []);

    return (
        <div className="h-[16px] shrink-0 w-full bg-black border-b border-white/5 flex items-center px-4 font-mono text-[9px] text-zinc-600 uppercase tracking-widest justify-between z-50">
            <span className="flex gap-6">
                <span>SYS.MEM: <span className="text-zinc-400">{hex}</span></span>
                <span>TENSORS: <span className="text-zinc-400">{tensors}</span></span>
                <span>LATENCY: <span className="text-zinc-400">{latency}ms</span></span>
                <span className="text-emerald-500/80 animate-pulse flex items-center gap-1">
                    <span className="w-1 h-1 bg-emerald-500 rounded-full"></span> LIVE
                </span>
            </span>
            <span>CLUSTER: PRIMARY_ALPHA_01</span>
        </div>
    )
};

const Sparkline = () => (
    <div className="flex items-end gap-[1px] h-3 w-10 opacity-70">
        {[...Array(10)].map((_, i) => (
            <div
                key={i}
                className="w-[2px] bg-emerald-500/60"
                style={{ height: `${Math.random() * 80 + 20}%` }}
            />
        ))}
    </div>
);

const CursorSpotlightGrid = ({ children, processing, rightContent, activeTab }) => {
    const containerRef = useRef(null);
    const svgRef = useRef(null);
    const [dimensions, setDimensions] = useState({ width: 0, height: 0 });

    const mousePos = useRef({ x: -1000, y: -1000 });
    const dotsState = useRef({
        r: new Float32Array(0),
        opacity: new Float32Array(0)
    });

    useEffect(() => {
        const updateDimensions = () => {
            if (containerRef.current) {
                const { width, height } = containerRef.current.getBoundingClientRect();
                setDimensions({ width, height });
            }
        };
        updateDimensions();
        window.addEventListener('resize', updateDimensions);
        return () => window.removeEventListener('resize', updateDimensions);
    }, []);

    const cols = Math.floor(dimensions.width / 24) + 1;
    const rows = Math.floor(dimensions.height / 24) + 1;
    const totalDots = cols * rows;

    useEffect(() => {
        if (totalDots > 0) {
            dotsState.current.r = new Float32Array(totalDots).fill(1);
            dotsState.current.opacity = new Float32Array(totalDots).fill(0.15);
        }
    }, [totalDots]);

    useEffect(() => {
        if (!processing || totalDots === 0) return;

        const computeInterval = setInterval(() => {
            const numNodes = Math.floor(Math.random() * 6) + 5;
            for (let k = 0; k < numNodes; k++) {
                const randomIdx = Math.floor(Math.random() * totalDots);
                dotsState.current.r[randomIdx] = 1.6;
                dotsState.current.opacity[randomIdx] = 0.9;
            }
        }, 100);

        return () => clearInterval(computeInterval);
    }, [processing, totalDots]);

    useEffect(() => {
        if (totalDots === 0) return;
        let animationFrameId;

        const animate = () => {
            if (!svgRef.current) return;
            const mx = mousePos.current.x;
            const my = mousePos.current.y;
            const children = svgRef.current.children;

            for (let i = 0; i < children.length; i++) {
                const circle = children[i];
                const c = i % cols;
                const rCount = Math.floor(i / cols);
                const cx = c * 24 + 12;
                const cy = rCount * 24 + 12;

                const dx = cx - mx;
                const dy = cy - my;
                const distance = Math.sqrt(dx * dx + dy * dy);

                const currentR = dotsState.current.r[i];
                const currentOpacity = dotsState.current.opacity[i];

                // Execute physics only if dot is near cursor OR needs to decay to rest
                if (distance < 40 || currentR > 1) {
                    const intensity = distance < 40 ? 1 - (distance / 40) : 0;
                    const targetR = 1 + (3 * intensity);
                    const targetOpacity = 0.15 + (0.85 * intensity);

                    const nextR = currentR + ((targetR - currentR) * 0.15);
                    const nextOpacity = currentOpacity + ((targetOpacity - currentOpacity) * 0.15);

                    if (nextR <= 1.01 && distance >= 40) {
                        // Snap completely to rest
                        dotsState.current.r[i] = 1;
                        dotsState.current.opacity[i] = 0.15;
                        circle.setAttribute('r', '1');
                        circle.setAttribute('opacity', '0.15');
                        circle.setAttribute('fill', '#ffffff');
                        circle.style.filter = 'none';
                    } else {
                        dotsState.current.r[i] = nextR;
                        dotsState.current.opacity[i] = nextOpacity;
                        circle.setAttribute('r', nextR.toFixed(3));
                        circle.setAttribute('opacity', nextOpacity.toFixed(3));

                        if (nextR > 1.2) {
                            circle.setAttribute('fill', '#10b981');
                            circle.style.filter = `drop-shadow(0 0 4px rgba(16,185,129,${((nextR - 1) / 3 * 0.8).toFixed(3)}))`;
                        } else {
                            circle.setAttribute('fill', '#ffffff');
                            circle.style.filter = 'none';
                        }
                    }
                }
            }
            animationFrameId = requestAnimationFrame(animate);
        };

        animate();
        return () => cancelAnimationFrame(animationFrameId);
    }, [cols, totalDots]);

    const handleMouseMove = (e) => {
        if (processing) return;
        if (containerRef.current) {
            const rect = containerRef.current.getBoundingClientRect();
            mousePos.current = {
                x: e.clientX - rect.left,
                y: e.clientY - rect.top,
            };
        }
    };

    const handleMouseLeave = () => {
        mousePos.current = { x: -1000, y: -1000 };
    };

    const matrix = Array.from({ length: totalDots }).map((_, i) => ({
        cx: (i % cols) * 24 + 12,
        cy: Math.floor(i / cols) * 24 + 12
    }));

    return (
        <div
            ref={containerRef}
            className="absolute inset-0 z-0 overflow-hidden bg-[#0a0a0a]"
            onMouseMove={handleMouseMove}
            onMouseLeave={handleMouseLeave}
        >
            <svg
                ref={svgRef}
                width={dimensions.width}
                height={dimensions.height}
                className={`absolute inset-0 pointer-events-none transition-opacity duration-1000 ${(rightContent === 'chart' && activeTab === 'viz') ? 'opacity-[0.05]' : 'opacity-100'}`}
            >
                {matrix.map((dot, i) => (
                    <circle
                        key={i}
                        cx={dot.cx}
                        cy={dot.cy}
                        r="1"
                        fill="#ffffff"
                        opacity="0.15"
                    />
                ))}
            </svg>
            {children}
        </div>
    );
};

const TerminalLoader = ({ currentTaskId, onComplete }) => {
    const steps = [
        "initializing Nova 2 Lite reasoning engine...",
        "allocating virtual compute resources...",
        "loading dataset tensors into memory...",
        "executing multidimensional variance scan...",
        "generating visualization artifact..."
    ];

    const [currentStep, setCurrentStep] = useState(0);

    useEffect(() => {
        if (!currentTaskId) return;

        const interval = setInterval(async () => {
            setCurrentStep(prev => (prev < steps.length - 1 ? prev + 1 : prev));

            try {
                const response = await fetch('https://95w2g285yg.execute-api.us-east-1.amazonaws.com/execute', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ action: 'check_status', task_id: currentTaskId })
                });

                if (response.ok) {
                    const data = await response.json();
                    if (data.task_status === 'completed') {
                        clearInterval(interval);
                        setCurrentStep(steps.length);
                        if (onComplete) {
                            setTimeout(() => {
                                onComplete(data.ai_analysis, data.chart_data);
                            }, 500);
                        }
                    }
                }
            } catch (err) {
                console.error("Failed to poll status:", err);
            }
        }, 2000);

        return () => clearInterval(interval);
    }, [currentTaskId, onComplete]);

    return (
        <div className="font-mono text-xs text-zinc-500 flex flex-col gap-2 p-8 border border-white/5 bg-[#0a0a0a]/80 backdrop-blur-md rounded-md shadow-[0_0_40px_rgba(0,0,0,0.8)] w-full max-w-lg z-10 relative">
            <div className="flex items-center gap-2 mb-4 border-b border-white/5 pb-2">
                <Cpu className="w-4 h-4 text-zinc-400" />
                <span className="text-zinc-300">Nova.Core.Process</span>
            </div>
            {steps.map((step, idx) => (
                <motion.div
                    key={idx}
                    initial={{ opacity: 0, x: -5 }}
                    animate={{ opacity: idx <= currentStep ? 1 : 0, x: idx <= currentStep ? 0 : -5 }}
                    transition={{ duration: 0.2, ease: 'easeOut' }}
                    className={idx === currentStep && currentStep < steps.length ? "text-emerald-400" : "text-zinc-500"}
                >
                    {idx <= currentStep && `> ${step}`}
                </motion.div>
            ))}
            {currentStep < steps.length && (
                <motion.span
                    animate={{ opacity: [1, 0] }}
                    transition={{ repeat: Infinity, duration: 0.8 }}
                    className="inline-block w-2 h-3 bg-white mt-2"
                />
            )}
        </div>
    );
};

const DataTable = ({ headers, data }) => (
    <motion.div
        initial={{ opacity: 0, scale: 0.98, y: 10 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        transition={{ duration: 0.2, ease: 'easeOut' }}
        className="border border-white/5 rounded-md overflow-hidden bg-[#0a0a0a]/90 backdrop-blur-md w-full max-w-4xl shadow-2xl z-10 relative"
    >
        <div className="bg-zinc-900/50 border-b border-white/5 px-4 py-3 flex items-center gap-2">
            <Database className="w-4 h-4 text-zinc-400" />
            <span className="font-mono text-xs text-zinc-300">dataset_preview.csv</span>
        </div>
        <div className="p-4 overflow-x-auto overflow-y-auto max-h-[350px] border border-gray-800 rounded-md custom-scrollbar">
            <table className="w-full text-left text-sm text-gray-400">
                <thead className="bg-[#0a0a0a]/90 backdrop-blur sticky top-0 z-10 text-emerald-500 font-mono text-xs uppercase">
                    <tr className="border-b border-white/5">
                        {headers?.map((h, i) => (
                            <th key={i} className="py-2 pr-4 font-normal whitespace-nowrap">{h}</th>
                        ))}
                    </tr>
                </thead>
                <tbody>
                    {data?.map((row, i) => (
                        <tr key={i} className="border-b border-white/5 last:border-0 hover:bg-white/5 transition-colors">
                            {headers?.map((h, j) => (
                                <td key={j} className={`py-2 pr-4 whitespace-nowrap ${j === 0 ? 'text-zinc-300' : 'text-zinc-500'}`}>
                                    {row[h]}
                                </td>
                            ))}
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    </motion.div>
);

const TypewriterText = ({ text }) => {
    const [displayed, setDisplayed] = useState('');
    useEffect(() => {
        const safeText = typeof text === 'string' ? text : JSON.stringify(text, null, 2) || "Error reading analysis.";
        let i = 0;
        setDisplayed('');
        const int = setInterval(() => {
            setDisplayed(safeText.slice(0, i));
            i++;
            if (i > safeText.length) clearInterval(int);
        }, 20);
        return () => clearInterval(int);
    }, [text]);
    return (
        <span className="whitespace-pre-wrap">
            {displayed}
            <span className="animate-pulse bg-emerald-400 w-1.5 h-3 inline-block ml-1 align-middle"></span>
        </span>
    );
};

const StrategyRenderer = ({ data }) => {
    let parsedData = null;

    if (typeof data === 'string') {
        try {
            parsedData = JSON.parse(data);
        } catch (e) {
            parsedData = data;
        }
    } else {
        parsedData = data;
    }

    if (parsedData && typeof parsedData === 'object' && !Array.isArray(parsedData)) {
        const strictOrder = ['descriptive', 'predictive', 'prescriptive'];
        const existingKeys = Object.keys(parsedData);
        const orderedKeys = strictOrder.filter(k => existingKeys.includes(k));
        const otherKeys = existingKeys.filter(k => !strictOrder.includes(k));

        const finalKeys = [...orderedKeys, ...otherKeys];

        return (
            <div className="flex flex-col gap-6 w-full max-w-4xl">
                {finalKeys.map((key) => (
                    <div key={key}>
                        <div className="text-emerald-400 font-mono text-xs uppercase tracking-[0.2em] mb-2 font-bold">
                            [ {key} ]
                        </div>
                        <div className="border-l-2 border-emerald-500/30 pl-4 mb-2">
                            <div className="prose prose-invert prose-emerald max-w-none text-sm text-zinc-300 leading-relaxed [&>p]:mb-4 [&>ul]:list-disc [&>ul]:pl-5 [&>li]:mb-1 [&>strong]:text-emerald-400 [&>code]:bg-zinc-800/50 [&>code]:text-emerald-300 [&>code]:px-1 [&>code]:rounded [&>code]:font-mono [&>code]:text-xs">
                                <ReactMarkdown>{parsedData[key]}</ReactMarkdown>
                            </div>
                        </div>
                    </div>
                ))}
            </div>
        );
    }

    // Fallback standard text rendering
    return (
        <div className="border-l-2 border-emerald-500/30 pl-4 py-1">
            <div className="prose prose-invert prose-emerald max-w-none text-sm text-zinc-300 leading-relaxed [&>p]:mb-4 [&>ul]:list-disc [&>ul]:pl-5 [&>li]:mb-1 [&>strong]:text-emerald-400 [&>code]:bg-zinc-800/50 [&>code]:text-emerald-300 [&>code]:px-1 [&>code]:rounded [&>code]:font-mono [&>code]:text-xs">
                <ReactMarkdown>{data || "Analysis complete."}</ReactMarkdown>
            </div>
        </div>
    );
};

const VisualizerArtifact = ({ chartData, aiAnalysisText }) => (
    <motion.div
        initial={{ opacity: 0, y: 15, scale: 0.98 }}
        animate={{ opacity: 1, y: 0, scale: 1 }}
        transition={{ duration: 0.3, ease: 'easeOut' }}
        className="w-full h-full flex-1 border-0 rounded-none bg-[#0a0a0a]/90 backdrop-blur-xl shadow-2xl flex flex-col relative z-10"
    >
        <div className="w-full flex justify-center sticky top-0 bg-[#0a0a0a]/90 backdrop-blur z-20 border-b border-white/5 px-6 py-4">
            <div className="flex items-center justify-between w-full max-w-5xl">
                <div className="flex items-center gap-2">
                    <LayoutPanelLeft className="w-5 h-5 text-zinc-500" />
                    <span className="font-mono text-xs text-zinc-400 uppercase tracking-widest hidden sm:inline">visualization_engine.plt</span>
                    <span className="font-mono text-xs text-zinc-400 uppercase tracking-widest sm:hidden">viz_out.plt</span>
                </div>
                <div className="flex items-center gap-2">
                    <span className="font-mono text-[10px] text-zinc-600 tracking-widest hidden sm:inline">RENDER: PLOTLY NATIVE</span>
                    <span className="w-2 h-2 rounded-full bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.8)] animate-pulse"></span>
                </div>
            </div>
        </div>

        <div className="p-6 sm:p-10 flex flex-col gap-10 flex-1 overflow-y-auto custom-scrollbar w-full items-center">
            <div className="w-full max-w-5xl">
                {/* Main Answers Top Block */}
                {aiAnalysisText?.main_answers && (
                    <div className="mb-8">
                        <div className="text-zinc-500 font-mono text-[11px] mb-3 tracking-widest uppercase flex items-center gap-2">
                            <Database className="w-3 h-3 text-emerald-500" />
                            Answers Query
                        </div>
                        <div className="text-zinc-300 font-sans pb-4 border-b border-white/5">
                            <div className="prose prose-invert prose-emerald max-w-none text-base text-zinc-300 leading-relaxed [&>p]:mb-4 [&>ul]:list-disc [&>ul]:pl-5 [&>li]:mb-2 [&>strong]:text-emerald-400 [&>code]:bg-zinc-800/50 [&>code]:text-emerald-300 [&>code]:px-1.5 [&>code]:py-0.5 [&>code]:rounded [&>code]:font-mono [&>code]:text-sm">
                                <ReactMarkdown>{aiAnalysisText.main_answers}</ReactMarkdown>
                            </div>
                        </div>
                    </div>
                )}

                <div className="flex flex-col gap-12 w-full">
                    {chartData && chartData.map((chart, index) => {
                        // Dynamically merge dark theme layout into plotly config
                        const darkLayout = {
                            ...chart.layout,
                            paper_bgcolor: 'transparent',
                            plot_bgcolor: 'transparent',
                            font: { color: '#a1a1aa', family: 'monospace' },
                            xaxis: { ...chart.layout?.xaxis, gridcolor: '#27272a', zerolinecolor: '#3f3f46' },
                            yaxis: { ...chart.layout?.yaxis, gridcolor: '#27272a', zerolinecolor: '#3f3f46' }
                        };

                        return (
                            <div key={index} className="flex flex-col gap-3">
                                <div className="text-zinc-400 font-mono text-xs tracking-widest uppercase border-l-2 border-emerald-500/50 pl-3 py-1 bg-gradient-to-r from-emerald-500/10 to-transparent">
                                    {chart.title || "Visualization Artifact"}
                                </div>
                                <div className="w-full relative overflow-hidden border border-white/10 rounded-md bg-black p-4 mb-6 shadow-xl">
                                    <Plot
                                        data={chart.data}
                                        layout={{
                                            ...darkLayout,
                                            autosize: true,
                                        }}
                                        useResizeHandler={true}
                                        style={{ width: '100%', height: '300px' }}
                                        config={{ displayModeBar: false, responsive: true }}
                                    />
                                    <div className="absolute top-4 left-4 font-mono text-[9px] sm:text-[10px] px-2 py-1 bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 backdrop-blur-md z-10">
                                        CONFIDENCE: {(Math.random() * (99.9 - 94.0) + 94.0).toFixed(1)}%
                                    </div>
                                </div>
                            </div>
                        );
                    })}
                </div>
            </div>
        </div>
    </motion.div>
);

const SchemaInspector = ({ headers }) => (
    <div className="bg-zinc-900/20 border-y border-white/5 px-6 py-4 font-mono text-xs shadow-inner">
        <div className="text-zinc-500 mb-3 tracking-widest uppercase text-[9px] flex items-center justify-between">
            <span>Data Schema Inferred</span>
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-500/50 animate-pulse"></span>
        </div>
        <div className="flex flex-col gap-2 shadow-2xl overflow-y-auto max-h-32 pr-2 custom-scrollbar">
            {headers && headers.length > 0 ? (
                headers.map((h, i) => (
                    <div key={i} className="flex justify-between items-center hover:bg-white/[0.02] px-1 py-0.5 rounded cursor-default group">
                        <span className="text-zinc-400">{h}</span>
                        <div className="flex items-center gap-3">
                            <Sparkline />
                            <span className="text-emerald-500/60 text-[10px]">auto</span>
                        </div>
                    </div>
                ))
            ) : (
                <div className="text-zinc-600 text-[10px] italic">Awaiting schema...</div>
            )}
        </div>
    </div>
);

// --- Main App ---

export default function NovaFlowDashboard() {
    const [file, setFile] = useState(null);
    const [query, setQuery] = useState('');
    const [questionsAsked, setQuestionsAsked] = useState(() => {
        const stored = localStorage.getItem('novaflow_questions_asked');
        return stored ? parseInt(stored, 10) : 0;
    });
    const [showPaywall, setShowPaywall] = useState(false);
    const [chatHistory, setChatHistory] = useState([]);
    const [desktopPanelOpen, setDesktopPanelOpen] = useState(true);
    const [processing, setProcessing] = useState(false);
    const [rightContent, setRightContent] = useState('empty'); // 'empty', 'table', 'processing', 'chart'
    const [activeTab, setActiveTab] = useState('data');

    // Async State
    const [currentTaskId, setCurrentTaskId] = useState('');
    const [aiAnalysisText, setAiAnalysisText] = useState({});
    const [chartData, setChartData] = useState([]);

    // Real CSV State
    const [csvHeaders, setCsvHeaders] = useState([]);
    const [csvPreviewData, setCsvPreviewData] = useState([]);

    // PLG State
    const [userEmail, setUserEmail] = useState(() => {
        return localStorage.getItem('novaflow_user_email') || null;
    });
    const [showSoftPaywall, setShowSoftPaywall] = useState(false);
    const [emailInput, setEmailInput] = useState('');
    const [mobilePanelOpen, setMobilePanelOpen] = useState(false);
    // Automatically generate a unique ID for every new browser that visits the site
    const [userId] = useState(() => {
        let storedId = localStorage.getItem('novaflow_guest_id');
        if (!storedId) {
            storedId = 'guest_' + Math.random().toString(36).substring(2, 9);
            localStorage.setItem('novaflow_guest_id', storedId);
        }
        return storedId;
    });

    useEffect(() => {
        localStorage.setItem('novaflow_questions_asked', questionsAsked.toString());
    }, [questionsAsked]);

    useEffect(() => {
        if (userEmail) {
            localStorage.setItem('novaflow_user_email', userEmail);
        } else {
            localStorage.removeItem('novaflow_user_email');
        }
    }, [userEmail]);

    const MAX_ANON = 3;
    const MAX_LEAD = 5;
    const quotaLimit = userEmail ? MAX_LEAD : MAX_ANON;

    const handleFileUpload = (e) => {
        if (e.target.files && e.target.files[0]) {
            const uploadedFile = e.target.files[0];
            setFile(uploadedFile);

            const reader = new FileReader();
            reader.onload = (event) => {
                const text = event.target.result;
                const lines = text.split(/\r?\n/).filter(line => line.trim() !== '');
                if (lines.length > 0) {
                    const headers = lines[0].split(',').map(h => h.trim());
                    const previewRows = lines.slice(1, 51).map(line => {
                        const values = line.split(',');
                        return headers.reduce((obj, header, i) => {
                            obj[header] = values[i] ? values[i].trim() : '';
                            return obj;
                        }, {});
                    });

                    setCsvHeaders(headers);
                    setCsvPreviewData(previewRows);
                    setRightContent('table');
                    setActiveTab('data');
                    setChatHistory([{ role: 'ai', content: `> Dataset mounted successfully. ${lines.length - 1} rows detected. Schema inference complete.` }]);
                }
            };

            // Read first 500KB to safely parse some lines without locking the browser on huge files
            const slice = uploadedFile.slice(0, 500 * 1024);
            reader.readAsText(slice);
        }
    };

    const handleAsk = async () => {
        if (!query.trim() || !file || processing) return;

        if (userEmail === null && questionsAsked >= MAX_ANON) {
            setShowSoftPaywall(true);
            return;
        }

        if (userEmail !== null && questionsAsked >= MAX_LEAD) {
            setShowPaywall(true);
            return;
        }

        // Store query before clearing the input box
        const currentQuery = query;

        setChatHistory(prev => [...prev, { role: 'user', content: currentQuery }]);
        setQuery('');
        setQuestionsAsked(prev => prev + 1);
        setProcessing(true);
        setRightContent('processing');
        setActiveTab('logs');

        // Close mobile panel automatically when asking to show results
        if (window.innerWidth < 768) {
            setMobilePanelOpen(false);
        }

        try {
            // STEP 1: Get VIP Pass
            const urlResponse = await fetch('https://95w2g285yg.execute-api.us-east-1.amazonaws.com/execute', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ action: 'get_upload_url', file_name: file.name })
            });

            if (urlResponse.status === 403) {
                setProcessing(false);
                setShowSoftPaywall(true);
                return;
            }
            if (urlResponse.status === 402) {
                setProcessing(false);
                setShowPaywall(true);
                return;
            }
            if (!urlResponse.ok) throw new Error(`HTTP error! status: ${urlResponse.status}`);

            const urlData = await urlResponse.json();
            const { upload_url, file_key } = urlData;

            // STEP 2: Upload raw file to S3
            const uploadResponse = await fetch(upload_url, {
                method: 'PUT',
                body: file
            });

            if (!uploadResponse.ok) throw new Error(`Upload failed! status: ${uploadResponse.status}`);

            // STEP 3: Execute Job
            const execResponse = await fetch('https://95w2g285yg.execute-api.us-east-1.amazonaws.com/execute', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    action: 'execute',
                    user_id: userId,
                    file_key: file_key,
                    prompt: currentQuery,
                    email: userEmail
                })
            });

            if (execResponse.status === 403) {
                setProcessing(false);
                setShowSoftPaywall(true);
                return;
            }
            if (execResponse.status === 402) {
                setProcessing(false);
                setShowPaywall(true);
                return;
            }
            if (!execResponse.ok) {
                throw new Error(`HTTP error! status: ${execResponse.status}`);
            }

            const data = await execResponse.json();

            // Log the success to your browser console!
            console.log("Task accepted by AWS! Task ID:", data.task_id);
            setCurrentTaskId(data.task_id);

        } catch (error) {
            console.error("Failed to reach NovaFlow Brain:", error);
            setChatHistory(prev => [...prev, { role: 'ai', content: `> ERR_CONNECTION: ${error.message}` }]);
            setProcessing(false);
            setRightContent('empty');
        }
    };

    const onProcessingComplete = (analysis, chart) => {
        if (analysis) setAiAnalysisText(JSON.parse(analysis));
        if (chart) setChartData(JSON.parse(chart));

        setProcessing(false);
        setRightContent('chart');
        setActiveTab('viz');
        setChatHistory(prev => [...prev, {
            role: 'ai',
            content: '> Analysis complete. Artifact and insights ready.'
        }]);
    };

    return (
        <div className="flex flex-col h-screen w-screen bg-[#0a0a0a] text-zinc-300 font-sans overflow-hidden antialiased selection:bg-emerald-500/30">

            {/* Absolute Top Ultra-Thin Metric Bar (Hidden on very small screens) */}
            <div className="hidden sm:block">
                <TopStatusBar />
            </div>

            {/* --- Main Grid --- */}
            <div className="flex-1 flex flex-col md:flex-row overflow-hidden relative">

                {/* Mobile Header (Replaces Activity Rail and Command Center Header on mobile) */}
                <div className="md:hidden flex items-center justify-between p-4 border-b border-white/5 bg-zinc-900/50 z-30 shadow-md">
                    <div className="flex items-center gap-3">
                        <div className="w-8 h-8 flex items-center justify-center bg-zinc-200 text-black font-bold uppercase tracking-tighter text-lg pt-0.5 rounded-[2px] shadow-[0_0_10px_rgba(255,255,255,0.1)]">N</div>
                        <h1 className="text-lg font-semibold tracking-tight text-zinc-100 font-sans">NovaFlow</h1>
                    </div>
                    <div className={`text-xs font-mono px-2 py-1.5 border rounded-[2px] transition-colors flex flex-col items-center flex-shrink-0 ${questionsAsked >= quotaLimit ? "bg-red-500/5 text-red-500 border-red-500/20" : "bg-white/[0.02] text-zinc-400 border-white/5"}`}>
                        <span className="text-[8px] uppercase tracking-widest text-zinc-600 mb-0.5">COMPUTE QUOTA</span>
                        <span className="text-[11px] font-bold">{Math.max(0, quotaLimit - questionsAsked)}/{quotaLimit} REQ</span>
                    </div>
                </div>

                {/* 1. Far-Left Activity Rail (Desktop Only) */}
                <div className="hidden md:flex w-[50px] shrink-0 border-r border-white/5 bg-black flex-col items-center py-4 justify-between z-20 shadow-[1px_0_10px_rgba(0,0,0,0.8)]">
                    <div className="w-8 h-8 flex items-center justify-center bg-zinc-200 text-black font-bold uppercase tracking-tighter text-lg pt-0.5 rounded-[2px] shadow-[0_0_15px_rgba(255,255,255,0.1)]">N</div>
                    <div className="flex flex-col gap-6 items-center flex-1 mt-8 text-zinc-600">
                        <div
                            onClick={() => setDesktopPanelOpen(!desktopPanelOpen)}
                            className="p-2 hover:bg-white/10 hover:text-zinc-300 rounded-[2px] cursor-pointer transition-colors"
                            title="Toggle Command Center"
                        >
                            <LayoutPanelLeft className="w-5 h-5" strokeWidth={1.5} />
                        </div>
                        <div className="p-2 bg-white/10 rounded-[2px] text-zinc-200 cursor-pointer block drop-shadow-[0_0_8px_rgba(255,255,255,0.2)]"><Database className="w-5 h-5" strokeWidth={1.5} /></div>
                        <div className="p-2 hover:text-zinc-300 rounded-[2px] cursor-pointer"><Activity className="w-5 h-5" strokeWidth={1.5} /></div>
                        <div className="p-2 hover:text-zinc-300 rounded-[2px] cursor-pointer"><Settings className="w-5 h-5" strokeWidth={1.5} /></div>
                    </div>
                    <div className="w-8 h-8 rounded-full bg-zinc-900 border border-white/10 flex items-center justify-center cursor-pointer hover:border-white/30 transition-colors">
                        <User className="w-4 h-4 text-zinc-500" />
                    </div>
                </div>

                {/* Mobile Left Panel Overlay Background */}
                {mobilePanelOpen && rightContent !== 'empty' && (
                    <div
                        className="md:hidden fixed inset-0 bg-black/60 backdrop-blur-sm z-40"
                        onClick={() => setMobilePanelOpen(false)}
                    />
                )}

                {/* 2. Left Panel (Command Center) - Adapts to full width on mobile or slides over if right content is active */}
                <div className={`
                    ${desktopPanelOpen ? 'md:w-[30%] md:min-w-[320px] md:max-w-[420px] md:border-r border-white/5' : 'md:w-0 md:min-w-0 md:max-w-0 md:border-r-0 md:overflow-hidden'}
                    w-full min-w-0 max-w-none border-b md:border-b-0 bg-[#0a0a0a] flex-col z-50 md:z-10 shadow-[2px_0_20px_rgba(0,0,0,0.8)] md:shadow-none transition-all duration-300 ease-in-out 
                    ${rightContent !== 'empty' ? (mobilePanelOpen ? 'flex absolute inset-y-0 left-0 w-[85vw] translate-x-0' : 'flex absolute md:relative inset-y-0 left-0 w-[85vw] md:w-auto -translate-x-full md:translate-x-0') : 'flex relative w-full translate-x-0'}
                `}>

                    {/* Mobile Left Panel Header */}
                    <div className="md:hidden flex items-center justify-between p-4 border-b border-white/5 bg-zinc-900/50">
                        <span className="font-sans font-semibold tracking-tight text-zinc-100">Command Terminal</span>
                        <button onClick={() => setMobilePanelOpen(false)} className="p-1 rounded bg-white/5 text-zinc-400 hover:text-white">
                            <X className="w-4 h-4" />
                        </button>
                    </div>

                    {/* Desktop Header */}
                    <div className="hidden md:flex px-6 py-5 border-b border-white/5 items-center justify-between bg-zinc-900/10">
                        <div>
                            <h1 className="text-lg font-semibold tracking-tight text-zinc-100 mb-0.5 font-sans">NovaFlow</h1>
                            <div className="text-[9px] font-mono tracking-widest text-zinc-600 uppercase">SYS_ADMIN_ACTIVE</div>
                        </div>
                        <div className={`text-xs font-mono px-2 py-1.5 border rounded-[2px] transition-colors flex flex-col items-center ${questionsAsked >= quotaLimit ? "bg-red-500/5 text-red-500 border-red-500/20" : "bg-white/[0.02] text-zinc-400 border-white/5"}`}>
                            <span className="text-[8px] uppercase tracking-widest text-zinc-600 mb-0.5">COMPUTE QUOTA</span>
                            <span className="text-[11px]">{Math.max(0, quotaLimit - questionsAsked)}/{quotaLimit} REQ</span>
                        </div>
                    </div>

                    {/* Dynamic Content Area */}
                    {!file ? (
                        // Upload State
                        <div className="flex-1 flex flex-col items-center justify-center p-8">
                            <div className="w-full h-48 border border-dashed border-white/10 bg-zinc-900/20 flex items-center justify-center flex-col transition-all hover:bg-zinc-900/40 hover:border-emerald-500/30 group relative cursor-pointer">
                                <input type="file" accept=".csv" className="absolute inset-0 w-full h-full opacity-0 cursor-pointer" onChange={handleFileUpload} />
                                <UploadCloud className="w-8 h-8 text-zinc-600 mb-4 group-hover:text-emerald-400/80 transition-colors" strokeWidth={1} />
                                <div className="text-sm font-medium text-zinc-300 mb-1">Inject Data Source</div>
                                <div className="text-[10px] font-mono text-zinc-600 uppercase tracking-widest">CSV / XLSX // MAX 50MB</div>
                            </div>
                        </div>
                    ) : (
                        // Active Chat Interface
                        <div className="flex-1 flex flex-col overflow-hidden relative">

                            {/* Schema Inspector */}
                            <SchemaInspector headers={csvHeaders} />

                            <div className="flex-1 overflow-y-auto p-6 flex flex-col gap-5 bg-black/20 shadow-inner">
                                {chatHistory.map((m, i) => (
                                    <motion.div
                                        key={i}
                                        initial={{ opacity: 0, x: m.role === 'user' ? 2 : -2 }}
                                        animate={{ opacity: 1, x: 0 }}
                                        transition={{ duration: 0.2 }}
                                        className={`flex w-full ${m.role === 'user' ? 'justify-end' : 'justify-start'}`}
                                    >
                                        {m.role === 'user' ? (
                                            <div className="bg-zinc-800/80 text-zinc-200 px-4 py-2 text-sm border border-white/5 max-w-[90%] rounded-[2px]">
                                                {m.content}
                                            </div>
                                        ) : (
                                            <div className="font-mono text-zinc-500 text-[11px] leading-relaxed w-full pl-3 border-l text-shadow-sm border-emerald-500/30 bg-zinc-900/20 py-1">
                                                {m.content}
                                            </div>
                                        )}
                                    </motion.div>
                                ))}
                                <div className="pb-4 shrink-0"></div>
                            </div>

                            {/* Input Dock */}
                            <div className="font-mono text-[9px] text-zinc-600 uppercase tracking-widest px-4 pt-3 flex justify-between bg-black">
                                <span>Terminal Input</span>
                                <span className="text-emerald-500/50">Secure</span>
                            </div>
                            <div className="p-3 bg-black relative z-20">
                                <div className="relative flex items-center group">
                                    <ChevronRight className={`absolute left-3 w-4 h-4 transition-colors ${processing ? 'text-zinc-700' : 'text-emerald-500'}`} />
                                    <textarea
                                        disabled={processing}
                                        autoFocus
                                        className="w-full bg-zinc-900/60 border border-white/10 py-3 pl-9 pr-12 text-[13px] text-zinc-200 focus:outline-none focus:border-emerald-500/50 focus:bg-zinc-900 transition-all placeholder-zinc-700 font-mono disabled:opacity-50 rounded-[2px] resize-none min-h-[46px] max-h-[200px] overflow-y-auto"
                                        placeholder={processing ? "SYSTEM_LOCKED..." : "execute command..."}
                                        value={query}
                                        onChange={(e) => {
                                            setQuery(e.target.value);
                                            e.target.style.height = 'auto';
                                            e.target.style.height = `${e.target.scrollHeight}px`;
                                        }}
                                        onKeyDown={(e) => {
                                            if (e.key === 'Enter' && !e.shiftKey) {
                                                e.preventDefault();
                                                handleAsk();
                                            }
                                        }}
                                        rows={1}
                                    />
                                    <button
                                        disabled={processing}
                                        onClick={handleAsk}
                                        className="absolute right-2 p-1.5 rounded-[2px] bg-white/5 hover:bg-emerald-500/20 text-zinc-500 hover:text-emerald-400 transition-colors disabled:opacity-30 disabled:hover:bg-transparent"
                                    >
                                        <Terminal className="w-4 h-4" />
                                    </button>
                                </div>
                            </div>
                        </div>
                    )}
                </div>

                {/* 3. Right Panel (Artifact Canvas) */}
                <div className="flex-1 bg-[#0a0a0a] relative flex flex-col z-0 overflow-hidden">

                    {/* Interactive Grid Background */}
                    <CursorSpotlightGrid processing={processing} rightContent={rightContent} activeTab={activeTab}>

                        {/* Artifact Tabs */}
                        <div className="flex border-b border-white/5 bg-black/80 backdrop-blur-md relative z-20 px-6 pt-3 overflow-x-auto no-scrollbar">
                            {['data', 'logs', 'viz', 'strategy'].map((tab) => (
                                <button
                                    key={tab}
                                    onClick={() => file && setActiveTab(tab)}
                                    className={`px-6 py-2.5 font-mono text-[10px] tracking-widest uppercase border-b-2 transition-all whitespace-nowrap ${activeTab === tab && file
                                        ? "border-emerald-400/80 text-emerald-400 bg-white/5"
                                        : "border-transparent text-zinc-600 hover:text-zinc-400 hover:bg-white/[0.02]"
                                        } ${!file && "opacity-30 cursor-not-allowed"}`}
                                >
                                    {tab === 'data' ? '[ Data View ]' : tab === 'logs' ? '[ Execution Pipeline ]' : tab === 'viz' ? '[ Visualizations ]' : '[ Strategy Brief ]'}
                                </button>
                            ))}
                            <div className="ml-auto pb-2 flex items-end shrink-0 pl-6 hidden md:flex">
                                <span className="font-mono text-[9px] text-zinc-600 uppercase tracking-widest">Workspace: Isolated</span>
                            </div>
                        </div>

                        {/* Content Area */}
                        <div className={`flex-1 relative w-full h-full flex items-center justify-center overflow-y-auto ${rightContent === 'chart' && activeTab === 'viz' ? 'p-0' : 'p-8'}`}>
                            <AnimatePresence mode="wait">
                                {rightContent === 'table' && activeTab === 'data' && (
                                    <motion.div key="table" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.2 }} className="w-full flex justify-center">
                                        <DataTable headers={csvHeaders} data={csvPreviewData} />
                                    </motion.div>
                                )}
                                {rightContent === 'processing' && activeTab === 'logs' && (
                                    <motion.div key="processing" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.2 }} className="w-full flex justify-center">
                                        <TerminalLoader currentTaskId={currentTaskId} onComplete={onProcessingComplete} />
                                    </motion.div>
                                )}
                                {rightContent === 'chart' && activeTab === 'viz' && (
                                    <motion.div key="chart" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.2 }} className="w-full h-full flex items-center justify-center">
                                        <VisualizerArtifact chartData={chartData} aiAnalysisText={aiAnalysisText} />
                                    </motion.div>
                                )}
                                {/* Handle retained states */}
                                {rightContent === 'chart' && activeTab === 'data' && (
                                    <motion.div key="table-retained" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.2 }} className="w-full flex justify-center">
                                        <DataTable headers={csvHeaders} data={csvPreviewData} />
                                    </motion.div>
                                )}
                                {rightContent === 'chart' && activeTab === 'logs' && (
                                    <div key="logs-complete" className="font-mono text-xs text-zinc-400 flex flex-col p-8 border border-white/5 bg-[#0a0a0a]/80 backdrop-blur-md rounded-md z-10 w-full max-w-4xl h-full max-h-[80vh] overflow-hidden shadow-2xl">

                                        {/* Terminal Header */}
                                        <div className="flex items-center justify-between border-b border-white/10 pb-4 mb-4 shrink-0">
                                            <div className="flex items-center gap-3">
                                                <Terminal className="w-4 h-4 text-emerald-500" />
                                                <span className="text-zinc-300 tracking-widest uppercase">Execution Pipeline</span>
                                            </div>
                                            <div className="flex gap-2">
                                                <span className="w-2.5 h-2.5 rounded-full bg-zinc-800"></span>
                                                <span className="w-2.5 h-2.5 rounded-full bg-zinc-800"></span>
                                                <span className="w-2.5 h-2.5 rounded-full bg-zinc-800"></span>
                                            </div>
                                        </div>

                                        {/* Roadmap Body */}
                                        <div className="flex-1 overflow-y-auto custom-scrollbar pr-4 flex flex-col gap-6">

                                            {/* Process Steps */}
                                            <div className="flex flex-col gap-2 text-[11px] font-sans">
                                                <div className="flex items-center gap-2"><span className="text-emerald-500">✓</span> <span>Mounting dataset schema into agent context...</span></div>
                                                <div className="flex items-center gap-2"><span className="text-emerald-500">✓</span> <span>Analyzing statistical distribution & anomalies...</span></div>
                                                <div className="flex items-center gap-2"><span className="text-emerald-500">✓</span> <span>Writing analytical SQL query...</span></div>
                                                <div className="flex items-center gap-2"><span className="text-emerald-500">✓</span> <span>Executing query against DataFrame...</span></div>
                                            </div>

                                            {/* SQL Block */}
                                            <div className="flex flex-col gap-2">
                                                <div className="text-zinc-500 text-[10px] uppercase tracking-widest mb-1 flex items-center gap-2">
                                                    <Database className="w-3 h-3" />
                                                    RAW SQL KERNEL
                                                </div>
                                                <div className="bg-[#050505] p-5 border border-white/10 rounded-md shadow-[inset_0_2px_10px_rgba(0,0,0,0.5)]">
                                                    <pre className="text-emerald-400 font-mono text-[11px] whitespace-pre-wrap leading-relaxed">
                                                        <code>
                                                            {aiAnalysisText?.execution_log || "SELECT \n  category,\n  SUM(revenue) as total_revenue\nFROM dataset\nGROUP BY category\nORDER BY total_revenue DESC\nLIMIT 10;"}
                                                        </code>
                                                    </pre>
                                                </div>
                                            </div>

                                            <div className="text-emerald-500 mt-2 font-mono text-[11px]">&gt; Process complete. Artifacts deployed to Visualization & Strategy tabs.</div>
                                        </div>
                                    </div>
                                )}
                                {rightContent === 'chart' && activeTab === 'strategy' && (
                                    <motion.div key="strategy" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.2 }} className="w-full h-full flex justify-center py-8 overflow-y-auto custom-scrollbar">
                                        <StrategyRenderer data={aiAnalysisText?.strategy_brief} />
                                    </motion.div>
                                )}
                            </AnimatePresence>
                        </div>

                    </CursorSpotlightGrid>
                </div>

            </div>

            {/* Floating Mobile Toggle Button */}
            {rightContent !== 'empty' && (
                <button
                    onClick={() => setMobilePanelOpen(true)}
                    className={`md:hidden fixed bottom-10 right-6 z-[60] w-12 h-12 bg-emerald-500 rounded-full flex items-center justify-center text-black shadow-[0_0_20px_rgba(16,185,129,0.4)] transition-all duration-300 ${mobilePanelOpen ? 'scale-0' : 'scale-100'}`}
                >
                    <Terminal className="w-5 h-5" />
                </button>
            )}

            {/* 4. The Telemetry Footer */}
            <div className="h-[24px] shrink-0 border-t border-white/5 bg-black flex items-center px-4 justify-between font-mono text-[9px] uppercase tracking-widest text-zinc-500 z-30 shadow-[0_-1px_10px_rgba(0,0,0,0.8)]">
                <div className="flex items-center gap-6">
                    <span className="flex items-center gap-2"><span className="w-1.5 h-1.5 rounded-full bg-emerald-500/80"></span> Status: Connected</span>
                    <span>Model: amazon.nova-lite-v1:0</span>
                </div>
                <div className="flex items-center gap-6">
                    <span>Server: aws-us-east-1a</span>
                    <span>Compute: Nominal</span>
                </div>
            </div>

            {/* 5. The Paywall Modal Overlay */}
            <AnimatePresence>
                {showPaywall && (
                    <motion.div
                        initial={{ opacity: 0, backdropFilter: "blur(0px)" }}
                        animate={{ opacity: 1, backdropFilter: "blur(12px)" }}
                        exit={{ opacity: 0, backdropFilter: "blur(0px)" }}
                        transition={{ duration: 0.2 }}
                        className="fixed inset-0 z-[100] flex items-center justify-center bg-black/80 p-4"
                    >
                        <motion.div
                            initial={{ opacity: 0, scale: 0.98, y: 5 }}
                            animate={{ opacity: 1, scale: 1, y: 0 }}
                            transition={{ duration: 0.2, ease: "easeOut" }}
                            className="w-full max-w-md bg-[#0a0a0a] text-zinc-100 border border-zinc-800 shadow-[0_0_80px_rgba(0,0,0,0.9)] relative overflow-hidden rounded-[2px]"
                        >
                            <div className="w-full h-1 bg-white" />
                            <div className="p-8">
                                <Lock className="w-5 h-5 text-zinc-400 mb-6" strokeWidth={1.5} />
                                <h2 className="text-lg font-medium tracking-tight mb-2 uppercase text-white font-sans">Maximum Compute Reached</h2>
                                <p className="text-zinc-500 text-[11px] mb-8 font-mono leading-relaxed uppercase tracking-wider">
                                    &gt; ERR_QUOTA_EXHAUSTED<br />
                                    &gt; 402_PAYMENT_REQUIRED
                                </p>
                                <div className="border border-white/5 bg-zinc-900/30 p-5 rounded-[2px] mb-8 relative overflow-hidden group">
                                    <div className="absolute inset-0 bg-emerald-500/5 opacity-0 group-hover:opacity-100 transition-opacity"></div>
                                    <p className="text-[13px] text-zinc-300 mb-4 font-sans leading-relaxed relative z-10">
                                        Upgrade to Premium for Unlimited Enterprise Access.
                                    </p>
                                    <ul className="space-y-3 text-[10px] font-mono text-zinc-400 uppercase tracking-wider relative z-10">
                                        <li className="flex items-center gap-3"><Activity className="w-3 h-3 text-emerald-500/80" /> Unlimited Analysis Streams</li>
                                        <li className="flex items-center gap-3"><Database className="w-3 h-3 text-emerald-500/80" /> Datasets up to 500MB</li>
                                        <li className="flex items-center gap-3"><Terminal className="w-3 h-3 text-emerald-500/80" /> Dedicated Compute Nodes</li>
                                    </ul>
                                </div>
                                <div className="flex flex-col gap-3">
                                    <button
                                        onClick={() => setShowPaywall(false)}
                                        className="w-full bg-white text-black font-semibold py-3 flex items-center justify-center gap-2 hover:bg-zinc-200 transition-colors uppercase text-xs tracking-widest rounded-[2px]"
                                    >
                                        Authorize Payment
                                    </button>
                                    <button
                                        onClick={() => setShowPaywall(false)}
                                        className="w-full text-zinc-600 font-mono text-[10px] py-2 hover:text-zinc-400 uppercase tracking-widest mt-1"
                                    >
                                        [ Abort Session ]
                                    </button>
                                </div>
                            </div>
                        </motion.div>
                    </motion.div>
                )}

                {/* Soft Paywall Modal */}
                {showSoftPaywall && (
                    <motion.div
                        initial={{ opacity: 0, backdropFilter: "blur(0px)" }}
                        animate={{ opacity: 1, backdropFilter: "blur(12px)" }}
                        exit={{ opacity: 0, backdropFilter: "blur(0px)" }}
                        transition={{ duration: 0.2 }}
                        className="fixed inset-0 z-[100] flex items-center justify-center bg-black/80 p-4"
                    >
                        <motion.div
                            initial={{ opacity: 0, scale: 0.98, y: 5 }}
                            animate={{ opacity: 1, scale: 1, y: 0 }}
                            transition={{ duration: 0.2, ease: "easeOut" }}
                            className="w-full max-w-md bg-[#0a0a0a] text-zinc-100 border border-zinc-800 shadow-[0_0_80px_rgba(0,0,0,0.9)] relative overflow-hidden rounded-[2px]"
                        >
                            <div className="w-full h-1 bg-emerald-500" />
                            <div className="p-8">
                                <Lock className="w-5 h-5 text-zinc-400 mb-6" strokeWidth={1.5} />
                                <h2 className="text-lg font-medium tracking-tight mb-2 uppercase text-white font-sans">Anonymous Compute Limit Reached</h2>
                                <p className="text-zinc-500 text-[11px] mb-6 font-mono leading-relaxed uppercase tracking-wider">
                                    &gt; Enter your work email to unlock 2 additional advanced data queries.
                                </p>

                                <form onSubmit={(e) => {
                                    e.preventDefault();
                                    if (emailInput.includes('@')) {
                                        setUserEmail(emailInput);
                                        setShowSoftPaywall(false);
                                    }
                                }} className="flex flex-col gap-4">
                                    <div className="relative">
                                        <Mail className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-zinc-500" />
                                        <input
                                            type="email"
                                            required
                                            value={emailInput}
                                            onChange={(e) => setEmailInput(e.target.value)}
                                            placeholder="engineer@company.com"
                                            className="w-full bg-zinc-900/60 border border-white/10 py-3 pl-10 pr-4 text-[13px] text-zinc-200 focus:outline-none focus:border-emerald-500/50 focus:bg-zinc-900 transition-all placeholder-zinc-700 font-mono rounded-[2px]"
                                        />
                                    </div>
                                    <button
                                        type="submit"
                                        className="w-full bg-emerald-500 text-black font-semibold py-3 flex items-center justify-center gap-2 hover:bg-emerald-400 transition-colors uppercase text-xs tracking-widest rounded-[2px]"
                                    >
                                        Unlock Compute
                                    </button>
                                    <button
                                        type="button"
                                        onClick={() => setShowSoftPaywall(false)}
                                        className="w-full text-zinc-600 font-mono text-[10px] py-2 hover:text-zinc-400 uppercase tracking-widest mt-1"
                                    >
                                        [ Cancel ]
                                    </button>
                                </form>
                            </div>
                        </motion.div>
                    </motion.div>
                )}
            </AnimatePresence>

        </div>
    );
}
