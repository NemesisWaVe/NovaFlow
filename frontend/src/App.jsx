import React, { useState, useEffect, useRef } from 'react';
import { UploadCloud, Terminal, ChevronRight, Lock, Activity, Database, LayoutPanelLeft, Cpu, Settings, User } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';

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

const CursorSpotlightGrid = ({ children }) => {
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
                className="absolute inset-0 pointer-events-none"
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

const TerminalLoader = ({ onComplete }) => {
    const steps = [
        "initializing Nova 2 Lite reasoning engine...",
        "allocating virtual compute resources...",
        "loading dataset tensors into memory...",
        "executing multidimensional variance scan...",
        "generating visualization artifact..."
    ];

    const [currentStep, setCurrentStep] = useState(0);

    useEffect(() => {
        if (currentStep < steps.length) {
            const timer = setTimeout(() => {
                setCurrentStep(prev => prev + 1);
            }, 600);
            return () => clearTimeout(timer);
        } else if (onComplete) {
            const finishTimer = setTimeout(() => {
                onComplete();
            }, 500);
            return () => clearTimeout(finishTimer);
        }
    }, [currentStep, steps, onComplete]);

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

const DataTable = () => (
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
        <div className="p-4 overflow-x-auto">
            <table className="w-full text-left border-collapse text-xs font-mono text-zinc-400">
                <thead>
                    <tr className="border-b border-white/5 text-zinc-500">
                        <th className="py-2 pr-4 font-normal">id</th>
                        <th className="py-2 pr-4 font-normal">timestamp</th>
                        <th className="py-2 pr-4 font-normal">metric_alpha</th>
                        <th className="py-2 pr-4 font-normal">metric_beta</th>
                        <th className="py-2 font-normal">system_status</th>
                    </tr>
                </thead>
                <tbody>
                    {[1, 2, 3, 4, 5, 6, 7].map((row) => (
                        <tr key={row} className="border-b border-white/5 last:border-0 hover:bg-white/5 transition-colors">
                            <td className="py-2 pr-4 text-zinc-300">0x{(Math.random() * 10000).toFixed(0)}</td>
                            <td className="py-2 pr-4 text-zinc-500">2026-02-23T10:0{row}:00Z</td>
                            <td className="py-2 pr-4 text-zinc-300">{(Math.random() * 100).toFixed(4)}</td>
                            <td className="py-2 pr-4 flex items-center gap-1 text-emerald-400/80 hover:text-emerald-300">
                                <Activity className="w-3 h-3" />
                                +{(Math.random() * 50).toFixed(2)}
                            </td>
                            <td className="py-2 text-zinc-600">NOMINAL</td>
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
        let i = 0;
        setDisplayed('');
        const int = setInterval(() => {
            setDisplayed(text.slice(0, i));
            i++;
            if (i > text.length) clearInterval(int);
        }, 20);
        return () => clearInterval(int);
    }, [text]);
    return (
        <span>
            {displayed}
            <span className="animate-pulse bg-emerald-400 w-1.5 h-3 inline-block ml-1 align-middle"></span>
        </span>
    );
};

const VisualizerArtifact = () => (
    <motion.div
        initial={{ opacity: 0, y: 15, scale: 0.98 }}
        animate={{ opacity: 1, y: 0, scale: 1 }}
        transition={{ duration: 0.3, ease: 'easeOut' }}
        className="border border-white/5 rounded-md overflow-hidden bg-[#0a0a0a]/90 backdrop-blur-xl shadow-2xl w-full max-w-5xl z-10 flex flex-col md:flex-row h-auto md:h-[500px] relative"
    >
        {/* Visual Region (65%) */}
        <div className="w-full md:w-[65%] flex flex-col relative group">
            <div className="bg-zinc-900/30 border-b border-white/5 px-4 py-3 flex items-center justify-between z-20">
                <div className="flex items-center gap-2">
                    <LayoutPanelLeft className="w-4 h-4 text-zinc-500" />
                    <span className="font-mono text-[10px] text-zinc-400 uppercase tracking-widest hidden sm:inline">visualization_output.plt</span>
                    <span className="font-mono text-[10px] text-zinc-400 uppercase tracking-widest sm:hidden">viz_out.plt</span>
                </div>
                <div className="flex items-center gap-2">
                    <span className="font-mono text-[9px] text-zinc-600 tracking-widest hidden sm:inline">RENDER: ONLINE</span>
                    <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.8)] animate-pulse"></span>
                </div>
            </div>
            <div className="p-4 sm:p-6 flex flex-col gap-4 flex-1">
                <div className="flex justify-between items-end border-b border-white/5 pb-4">
                    <div>
                        <div className="text-zinc-600 font-mono text-[10px] mb-1 tracking-widest uppercase">Metric Correlation Map</div>
                        <div className="text-base sm:text-lg font-medium text-zinc-200 font-sans tracking-tight">Multidimensional Variance Analysis</div>
                    </div>
                    <div className="text-right text-emerald-400 font-mono text-xs sm:text-sm whitespace-nowrap hidden sm:block">+23.4% Δ V</div>
                </div>
                <div className="flex-1 w-full min-h-[250px] bg-black rounded flex flex-col justify-end relative overflow-hidden border border-white/5 cursor-crosshair">
                    <img
                        src="https://images.unsplash.com/photo-1551288049-bebda4e38f71?auto=format&fit=crop&w=800&q=80"
                        alt="Chart Data"
                        className="w-full h-full object-cover opacity-50 mix-blend-screen grayscale group-hover:grayscale-0 group-hover:opacity-80 transition-all duration-700"
                    />
                    <div className="absolute top-2 left-2 sm:top-4 sm:left-4 font-mono text-[9px] sm:text-[10px] px-2 py-1 bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 backdrop-blur-md">
                        CONFIDENCE: 98.2%
                    </div>
                    <div className="z-10 absolute bottom-2 left-2 right-2 sm:bottom-4 sm:left-4 sm:right-4 flex justify-between font-mono text-[8px] sm:text-[9px] text-zinc-500 uppercase tracking-widest">
                        <span>Y-Axis: Normalized Dist.</span>
                        <span className="hidden sm:inline">X-Axis: Timeline (T-0 to T-N)</span>
                    </div>
                </div>
            </div>
        </div>

        {/* Sonic Co-Pilot Region (35%) */}
        <div className="w-full md:w-[35%] border-t md:border-t-0 md:border-l border-white/5 bg-zinc-900/10 flex flex-col p-4 sm:p-6 overflow-hidden">
            <div className="text-[10px] font-mono text-emerald-400 mb-8 tracking-widest uppercase flex items-center gap-2">
                <span className="w-1 h-1 bg-emerald-400 rounded-full animate-pulse shadow-[0_0_8px_rgba(16,185,129,0.8)]"></span>
                Sonic Co-Pilot
            </div>

            {/* Voice Waveform */}
            <div className="flex items-end gap-1.5 h-16 mb-8 mt-4 px-2">
                {[1, 2, 3, 4, 5, 6, 7].map(i => (
                    <motion.div
                        key={i}
                        animate={{ height: ['20%', '100%', '30%', '80%', '20%'] }}
                        transition={{ repeat: Infinity, duration: Math.random() * 0.8 + 0.4, ease: 'easeInOut' }}
                        className="w-1.5 bg-gradient-to-t from-emerald-500/10 via-emerald-500/50 to-emerald-400 rounded-full shadow-[0_0_10px_rgba(16,185,129,0.3)]"
                    />
                ))}
            </div>

            {/* Typewriter Stream */}
            <div className="font-mono text-[11px] text-zinc-400 leading-relaxed border-l-2 border-emerald-500/30 pl-4 py-1">
                <TypewriterText text="Analysis complete. I've isolated a 23.4% anomaly in the variance metric. Distribution skews to the upper quartile. Recommend threshold adjustment." />
            </div>

            <div className="mt-auto font-mono text-[9px] text-zinc-600 uppercase tracking-widest flex items-center gap-2">
                <span className="w-1.5 h-1.5 border border-zinc-500 rounded-full"></span>
                Awaiting input...
            </div>
        </div>
    </motion.div>
);

const SchemaInspector = () => (
    <div className="bg-zinc-900/20 border-y border-white/5 px-6 py-4 font-mono text-xs shadow-inner">
        <div className="text-zinc-500 mb-3 tracking-widest uppercase text-[9px] flex items-center justify-between">
            <span>Data Schema Inferred</span>
            <span className="w-1.5 h-1.5 rounded-full bg-emerald-500/50 animate-pulse"></span>
        </div>
        <div className="flex flex-col gap-2 shadow-2xl">
            <div className="flex justify-between items-center hover:bg-white/[0.02] px-1 py-0.5 rounded cursor-default group">
                <span className="text-zinc-400">id</span>
                <div className="flex items-center gap-3">
                    <Sparkline />
                    <span className="text-emerald-500/60 text-[10px]">string</span>
                </div>
            </div>
            <div className="flex justify-between items-center hover:bg-white/[0.02] px-1 py-0.5 rounded cursor-default group">
                <span className="text-zinc-400">timestamp</span>
                <div className="flex items-center gap-3">
                    <Sparkline />
                    <span className="text-emerald-500/60 text-[10px]">datetime</span>
                </div>
            </div>
            <div className="flex justify-between items-center hover:bg-white/[0.02] px-1 py-0.5 rounded cursor-default group">
                <span className="text-zinc-400">revenue</span>
                <div className="flex items-center gap-3">
                    <Sparkline />
                    <span className="text-emerald-500/60 text-[10px]">float64</span>
                </div>
            </div>
            <div className="flex justify-between items-center hover:bg-white/[0.02] px-1 py-0.5 rounded cursor-default group">
                <span className="text-zinc-400">status</span>
                <div className="flex items-center gap-3">
                    <Sparkline />
                    <span className="text-emerald-500/60 text-[10px]">enum</span>
                </div>
            </div>
        </div>
    </div>
);

// --- Main App ---

export default function NovaFlowDashboard() {
    const [file, setFile] = useState(null);
    const [query, setQuery] = useState('');
    const [questionsAsked, setQuestionsAsked] = useState(0);
    const [showPaywall, setShowPaywall] = useState(false);
    const [chatHistory, setChatHistory] = useState([]);
    const [processing, setProcessing] = useState(false);
    const [rightContent, setRightContent] = useState('empty'); // 'empty', 'table', 'processing', 'chart'
    const [activeTab, setActiveTab] = useState('data');

    const MAX_FREE = 3;

    const handleFileUpload = (e) => {
        if (e.target.files && e.target.files[0]) {
            setFile(e.target.files[0]);
            setRightContent('table');
            setActiveTab('data');
            setChatHistory([{ role: 'ai', content: '> Dataset mounted successfully. 5,000 rows detected. Schema inference complete.' }]);
        }
    };

    const handleAsk = () => {
        if (!query.trim() || !file || processing) return;

        if (questionsAsked >= MAX_FREE) {
            setShowPaywall(true);
            return;
        }

        setChatHistory(prev => [...prev, { role: 'user', content: query }]);
        setQuery('');
        setQuestionsAsked(prev => prev + 1);
        setProcessing(true);
        setRightContent('processing');
        setActiveTab('logs');
    };

    const onProcessingComplete = () => {
        setProcessing(false);
        setRightContent('chart');
        setActiveTab('viz');
        setChatHistory(prev => [...prev, {
            role: 'ai',
            content: '> Analysis complete. 23.4% anomaly isolated. Artifact available.'
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
                    <div className={`text-xs font-mono px-2 py-1.5 border rounded-[2px] transition-colors flex flex-col items-center flex-shrink-0 ${questionsAsked >= MAX_FREE ? "bg-red-500/5 text-red-500 border-red-500/20" : "bg-white/[0.02] text-zinc-400 border-white/5"}`}>
                        <span className="text-[8px] uppercase tracking-widest text-zinc-600 mb-0.5">COMPUTE QUOTA</span>
                        <span className="text-[11px] font-bold">{MAX_FREE - questionsAsked}/{MAX_FREE} REQ</span>
                    </div>
                </div>

                {/* 1. Far-Left Activity Rail (Desktop Only) */}
                <div className="hidden md:flex w-[50px] shrink-0 border-r border-white/5 bg-black flex-col items-center py-4 justify-between z-20 shadow-[1px_0_10px_rgba(0,0,0,0.8)]">
                    <div className="w-8 h-8 flex items-center justify-center bg-zinc-200 text-black font-bold uppercase tracking-tighter text-lg pt-0.5 rounded-[2px] shadow-[0_0_15px_rgba(255,255,255,0.1)]">N</div>
                    <div className="flex flex-col gap-6 items-center flex-1 mt-8 text-zinc-600">
                        <div className="p-2 bg-white/10 rounded-[2px] text-zinc-200 cursor-pointer block drop-shadow-[0_0_8px_rgba(255,255,255,0.2)]"><Database className="w-5 h-5" strokeWidth={1.5} /></div>
                        <div className="p-2 hover:text-zinc-300 rounded-[2px] cursor-pointer"><Activity className="w-5 h-5" strokeWidth={1.5} /></div>
                        <div className="p-2 hover:text-zinc-300 rounded-[2px] cursor-pointer"><Settings className="w-5 h-5" strokeWidth={1.5} /></div>
                    </div>
                    <div className="w-8 h-8 rounded-full bg-zinc-900 border border-white/10 flex items-center justify-center cursor-pointer hover:border-white/30 transition-colors">
                        <User className="w-4 h-4 text-zinc-500" />
                    </div>
                </div>

                {/* 2. Left Panel (Command Center) - Adapts to full width on mobile or hides if Right Panel is active */}
                <div className={`w-full md:w-[30%] min-w-0 md:min-w-[320px] max-w-none md:max-w-[420px] border-b md:border-b-0 md:border-r border-white/5 bg-[#0a0a0a] flex-col z-10 relative shadow-[0_2px_10px_rgba(0,0,0,0.5)] md:shadow-none ${rightContent !== 'empty' && window.innerWidth < 768 ? 'hidden' : 'flex'}`}>

                    {/* Desktop Header */}
                    <div className="hidden md:flex px-6 py-5 border-b border-white/5 items-center justify-between bg-zinc-900/10">
                        <div>
                            <h1 className="text-lg font-semibold tracking-tight text-zinc-100 mb-0.5 font-sans">NovaFlow</h1>
                            <div className="text-[9px] font-mono tracking-widest text-zinc-600 uppercase">SYS_ADMIN_ACTIVE</div>
                        </div>
                        <div className={`text-xs font-mono px-2 py-1.5 border rounded-[2px] transition-colors flex flex-col items-center ${questionsAsked >= MAX_FREE ? "bg-red-500/5 text-red-500 border-red-500/20" : "bg-white/[0.02] text-zinc-400 border-white/5"}`}>
                            <span className="text-[8px] uppercase tracking-widest text-zinc-600 mb-0.5">COMPUTE QUOTA</span>
                            <span className="text-[11px]">{MAX_FREE - questionsAsked}/{MAX_FREE} REQ</span>
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
                            <SchemaInspector />

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
                                    <input
                                        type="text"
                                        disabled={processing}
                                        autoFocus
                                        className="w-full bg-zinc-900/60 border border-white/10 py-3 pl-9 pr-12 text-[13px] text-zinc-200 focus:outline-none focus:border-emerald-500/50 focus:bg-zinc-900 transition-all placeholder-zinc-700 font-mono disabled:opacity-50 rounded-[2px]"
                                        placeholder={processing ? "SYSTEM_LOCKED..." : "execute command..."}
                                        value={query}
                                        onChange={(e) => setQuery(e.target.value)}
                                        onKeyDown={(e) => e.key === 'Enter' && handleAsk()}
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
                    <CursorSpotlightGrid>

                        {/* Artifact Tabs */}
                        <div className="flex border-b border-white/5 bg-black/80 backdrop-blur-md relative z-20 px-6 pt-3">
                            {['data', 'logs', 'viz'].map((tab) => (
                                <button
                                    key={tab}
                                    onClick={() => file && setActiveTab(tab)}
                                    className={`px-6 py-2.5 font-mono text-[10px] tracking-widest uppercase border-b-2 transition-all ${activeTab === tab && file
                                        ? "border-emerald-400/80 text-emerald-400 bg-white/5"
                                        : "border-transparent text-zinc-600 hover:text-zinc-400 hover:bg-white/[0.02]"
                                        } ${!file && "opacity-30 cursor-not-allowed"}`}
                                >
                                    {tab === 'data' ? '[ Data View ]' : tab === 'logs' ? '[ Execution Logs ]' : '[ Visualization ]'}
                                </button>
                            ))}
                            <div className="ml-auto pb-2 flex items-end">
                                <span className="font-mono text-[9px] text-zinc-600 uppercase tracking-widest">Workspace: Isolated</span>
                            </div>
                        </div>

                        {/* Content Area */}
                        <div className="flex-1 relative w-full h-full flex items-center justify-center p-8 overflow-y-auto">
                            <AnimatePresence mode="wait">
                                {rightContent === 'table' && activeTab === 'data' && (
                                    <motion.div key="table" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.2 }} className="w-full flex justify-center">
                                        <DataTable />
                                    </motion.div>
                                )}
                                {rightContent === 'processing' && activeTab === 'logs' && (
                                    <motion.div key="processing" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.2 }} className="w-full flex justify-center">
                                        <TerminalLoader onComplete={onProcessingComplete} />
                                    </motion.div>
                                )}
                                {rightContent === 'chart' && activeTab === 'viz' && (
                                    <motion.div key="chart" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.2 }} className="w-full h-full flex items-center justify-center">
                                        <VisualizerArtifact />
                                    </motion.div>
                                )}
                                {/* Handle retained states */}
                                {rightContent === 'chart' && activeTab === 'data' && (
                                    <motion.div key="table-retained" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.2 }} className="w-full flex justify-center">
                                        <DataTable />
                                    </motion.div>
                                )}
                                {rightContent === 'chart' && activeTab === 'logs' && (
                                    <div key="logs-complete" className="font-mono text-xs text-zinc-500 flex flex-col gap-2 p-8 border border-white/5 bg-[#0a0a0a]/80 backdrop-blur-md rounded-md z-10">
                                        <span className="text-emerald-400">&gt; Process completed successfully.</span>
                                        <span>&gt; Artifact available in Visualization tab.</span>
                                        <span className="text-zinc-600 mt-4">&gt; Standing by...</span>
                                    </div>
                                )}
                            </AnimatePresence>
                        </div>

                    </CursorSpotlightGrid>
                </div>

            </div>

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
                                        Upgrade to Premium for Unlimited Nova 2 Lite Access.
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
            </AnimatePresence>

        </div>
    );
}
