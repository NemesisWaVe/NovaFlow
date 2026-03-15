import React, { useState, useEffect, useRef } from 'react';
import { UploadCloud, Terminal, ChevronRight, Lock, Activity, Database, LayoutPanelLeft, Cpu, Settings, User, X, MessageSquare, PanelLeftClose, Maximize2, Minimize2, Volume2, Mic, Trash2, FileText } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import ReactMarkdown from 'react-markdown';
import Plot from 'react-plotly.js';
import html2canvas from 'html2canvas';
import { jsPDF } from 'jspdf';
import { SignedIn, SignedOut, SignInButton, UserButton, useUser } from '@clerk/clerk-react';

// Edge Sound Engine (Web Audio API)

const playSFX = (type) => {
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.connect(gain);
        gain.connect(ctx.destination);

        if (type === 'activate') {
            osc.type = 'sine';
            osc.frequency.setValueAtTime(400, ctx.currentTime);
            osc.frequency.exponentialRampToValueAtTime(800, ctx.currentTime + 0.1);
            gain.gain.setValueAtTime(0.2, ctx.currentTime);
            gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.1);
            osc.start(); osc.stop(ctx.currentTime + 0.1);
        } else if (type === 'deactivate') {
            osc.type = 'sine';
            osc.frequency.setValueAtTime(800, ctx.currentTime);
            osc.frequency.exponentialRampToValueAtTime(300, ctx.currentTime + 0.15);
            gain.gain.setValueAtTime(0.2, ctx.currentTime);
            gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.15);
            osc.start(); osc.stop(ctx.currentTime + 0.15);
        } else if (type === 'success') {
            osc.type = 'sine';
            osc.frequency.setValueAtTime(880, ctx.currentTime); // High pitch ding
            osc.frequency.exponentialRampToValueAtTime(440, ctx.currentTime + 0.2);
            gain.gain.setValueAtTime(0, ctx.currentTime);
            gain.gain.linearRampToValueAtTime(0.2, ctx.currentTime + 0.05);
            gain.gain.linearRampToValueAtTime(0.01, ctx.currentTime + 0.3);
            osc.start(); osc.stop(ctx.currentTime + 0.3);
        } else if (type === 'ai_speak') {
            osc.type = 'triangle';
            osc.frequency.setValueAtTime(523.25, ctx.currentTime);
            gain.gain.setValueAtTime(0.1, ctx.currentTime);
            gain.gain.linearRampToValueAtTime(0.01, ctx.currentTime + 0.4);
            osc.start(); osc.stop(ctx.currentTime + 0.4);
        }
    } catch (e) { console.error("SFX error:", e); }
};

// Components

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

const CursorSpotlightGrid = ({ children, processing, rightContent, activeTab, voiceActive, isListening }) => {
    const containerRef = useRef(null);
    const svgRef = useRef(null);
    const [dimensions, setDimensions] = useState({ width: 0, height: 0 });

    const mousePos = useRef({ x: -1000, y: -1000 });
    const dotsState = useRef({
        r: new Float32Array(0),
        opacity: new Float32Array(0)
    });

    const voiceActiveRef = useRef(voiceActive);
    useEffect(() => {
        voiceActiveRef.current = voiceActive;
    }, [voiceActive]);

    const isListeningRef = useRef(isListening);
    useEffect(() => {
        isListeningRef.current = isListening;
    }, [isListening]);

    useEffect(() => {
        let observer;
        const updateDimensions = () => {
            if (containerRef.current) {
                const { width, height } = containerRef.current.getBoundingClientRect();
                setDimensions({ width, height });
            }
        };
        updateDimensions();

        if (containerRef.current) {
            observer = new ResizeObserver(() => {
                updateDimensions();
            });
            observer.observe(containerRef.current);
        }

        return () => {
            if (observer) observer.disconnect();
        };
    }, []);

    const cols = Math.floor(dimensions.width / 24) + 1;
    const rows = Math.floor(dimensions.height / 24) + 1;
    const totalDots = cols * rows;

    useEffect(() => {
        if (totalDots > 0) {
            const newR = new Float32Array(totalDots).fill(1);
            const newOpacity = new Float32Array(totalDots).fill(0.15);

            if (dotsState.current.r.length > 0) {
                const limit = Math.min(totalDots, dotsState.current.r.length);
                for (let i = 0; i < limit; i++) {
                    newR[i] = dotsState.current.r[i] || 1;
                    newOpacity[i] = dotsState.current.opacity[i] || 0.15;
                }
            }

            dotsState.current.r = newR;
            dotsState.current.opacity = newOpacity;
        }
    }, [totalDots]);

    useEffect(() => {
        if (!processing || totalDots === 0) return;

        const computeInterval = setInterval(() => {
            const numNodes = Math.floor(Math.random() * 6) + 5;
            for (let k = 0; k < numNodes; k++) {
                const randomIdx = Math.floor(Math.random() * totalDots);
                if (randomIdx < dotsState.current.r.length) {
                    dotsState.current.r[randomIdx] = 3.0;
                    dotsState.current.opacity[randomIdx] = 1.0;
                }
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
                if (i >= dotsState.current.r.length) continue; // Protect against array bounds during React resize flush

                const circle = children[i];
                const c = i % cols;
                const rCount = Math.floor(i / cols);
                const cx = c * 24 + 12;
                const cy = rCount * 24 + 12;

                const dx = cx - mx;
                const dy = cy - my;
                const distance = Math.sqrt(dx * dx + dy * dy);

                const currentR = dotsState.current.r[i] || 1;
                const currentOpacity = dotsState.current.opacity[i] || 0.15;

                let targetR = 1;
                let targetOpacity = 0.15;
                let targetColor = '#ffffff';
                let isHighlighted = false;

                // 1. Mouse Tracking (Restores the Emerald Hover)
                if (distance < 40) {
                    const intensity = 1 - (distance / 40);
                    targetR = 1 + (3 * intensity);
                    targetOpacity = 0.15 + (0.85 * intensity);
                    if (targetR > 1.2) {
                        targetColor = '#10b981';
                        isHighlighted = true;
                    }
                }

                // 2. Audio Waveform (Overrides Mouse if louder)
                const time = Date.now() / 1000;
                const isListen = isListeningRef.current;
                const isVoice = voiceActiveRef.current;

                if (isVoice || isListen) {
                    const midY = (dimensions.height / 2) + 120; // Shifted waveform downward dynamically
                    const nx = cx / dimensions.width;

                    const primaryFreq = Math.sin(nx * 15 - time * 4);
                    const secondaryFreq = Math.cos(nx * 35 + time * 6);
                    const noise = Math.random() * 0.2 + 0.8;
                    const bellCurve = Math.sin(nx * Math.PI);

                    const waveAmplitude = Math.abs(primaryFreq * secondaryFreq * noise) * 200 * bellCurve;
                    const distFromMidY = Math.abs(cy - midY);

                    if (distFromMidY < waveAmplitude) {
                        const intensity = 1 - (distFromMidY / waveAmplitude);
                        const waveR = 1 + (2.5 * intensity);

                        if (waveR > targetR) {
                            targetR = waveR;
                            targetOpacity = 0.2 + (0.8 * intensity);
                            targetColor = isListen ? '#06b6d4' : '#10b981';
                            isHighlighted = true;
                        }
                    }
                }

                const transitionSpeed = (isVoice || isListen) ? 0.4 : 0.15;

                const nextR = currentR + ((targetR - currentR) * transitionSpeed);
                const nextOpacity = currentOpacity + ((targetOpacity - currentOpacity) * transitionSpeed);

                dotsState.current.r[i] = nextR;
                dotsState.current.opacity[i] = nextOpacity;

                let renderColor = targetColor;
                if (!isHighlighted && nextR > 1.2) {
                    renderColor = '#10b981'; // Re-apply emerald glow for execution pipeline dots
                }

                circle.setAttribute('r', nextR.toFixed(3));
                circle.setAttribute('opacity', nextOpacity.toFixed(3));
                circle.setAttribute('fill', renderColor);

                if (isHighlighted || nextR > 1.2) {
                    const shadowColor = renderColor === '#06b6d4' ? 'rgba(6,184,212,' : 'rgba(16,185,129,';
                    circle.style.filter = `drop-shadow(0 0 4px ${shadowColor}${((nextR - 1) / 3 * 0.8).toFixed(3)})`;
                } else {
                    circle.style.filter = 'none';
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
        let pollCount = 0;
        const maxPolls = 15; // 30 seconds

        const interval = setInterval(async () => {
            pollCount++;
            setCurrentStep(prev => (prev < steps.length - 1 ? prev + 1 : prev));

            if (pollCount >= maxPolls) {
                clearInterval(interval);
                setCurrentStep(steps.length - 1);
                if (onComplete) setTimeout(() => onComplete(null, null, "ERR_TIMEOUT: Backend compute cluster unresponsive."), 500);
                return;
            }

            try {
                const response = await fetch(import.meta.env.VITE_AWS_API_URL || 'https://api.novaflow.local', {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ action: 'check_status', task_id: currentTaskId })
                });
                if (response.ok) {
                    const data = await response.json();
                    if (data.task_status === 'completed') {
                        clearInterval(interval);
                        setCurrentStep(steps.length);
                        if (onComplete) setTimeout(() => onComplete(data.ai_analysis, data.chart_data), 500);
                    } else if (data.task_status === 'failed') {
                        clearInterval(interval);
                        setCurrentStep(steps.length - 1);
                        if (onComplete) setTimeout(() => onComplete(null, null, "Pipeline Failed: " + (data.error_msg || "Unknown error")), 500);
                    }
                }
            } catch (err) { console.error("Failed to poll status:", err); }
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

const DataTable = ({ headers, data, isAudioActive }) => (
    <motion.div
        initial={{ opacity: 0, scale: 0.98, y: 10 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        transition={{ duration: 0.2, ease: 'easeOut' }}
        className={`border border-white/5 rounded-md overflow-hidden w-full max-w-4xl shadow-2xl z-10 relative transition-all duration-500 ${isAudioActive ? 'bg-transparent backdrop-blur-sm' : 'bg-[#0a0a0a]/90 backdrop-blur-md'}`}
    >
        <div className={`border-b border-white/5 px-4 py-3 flex items-center gap-2 transition-all duration-500 ${isAudioActive ? 'bg-transparent' : 'bg-zinc-900/50'}`}>
            <Database className="w-4 h-4 text-zinc-400" />
            <span className="font-mono text-xs text-zinc-300">dataset_preview.csv</span>
        </div>
        <div className={`px-4 pb-4 overflow-x-auto overflow-y-auto max-h-[350px] border border-gray-800 rounded-md custom-scrollbar relative transition-all duration-500 ${isAudioActive ? 'bg-transparent' : 'bg-[#0a0a0a]'}`}>
            <table className="w-full text-left text-sm text-gray-400">
                <thead className={`sticky top-0 z-20 text-emerald-500 font-mono text-xs uppercase shadow-sm transition-all duration-500 ${isAudioActive ? 'bg-transparent' : 'bg-[#0a0a0a]'}`}>
                    <tr className="border-b border-white/10">
                        {headers?.map((h, i) => (
                            <th key={i} className={`pt-4 pb-2 pr-4 font-normal whitespace-nowrap transition-all duration-500 ${isAudioActive ? 'bg-transparent' : 'bg-[#0a0a0a]'}`}>{h}</th>
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

const StrategyRenderer = ({ data, audioUrl, voiceActive, setVoiceActive, setIsChatOpen, playSFX }) => {
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

    const aiAnalysis = { strategy_brief: parsedData };
    const [isExporting, setIsExporting] = useState(false);
    const audioRef = useRef(null);

    useEffect(() => {
        return () => {
            if (audioRef.current) {
                audioRef.current.pause();
            }
            if (setVoiceActive) setVoiceActive(false);
        };
    }, [setVoiceActive]);

    const generatePDF = async () => {
        if (playSFX) playSFX('success');
        setIsExporting(true);

        try {
            // 1. Create an invisible, isolated Dark Mode container
            const container = document.createElement('div');
            container.style.position = 'absolute';
            container.style.left = '-9999px';
            container.style.top = '-9999px';
            container.style.width = '800px';
            container.style.backgroundColor = '#09090b';
            container.style.padding = '40px';
            container.style.fontFamily = 'monospace, sans-serif';

            // 2. Build the HTML manually so Tailwind and Browser Print settings can't ruin it
            const keys = ['diagnostic', 'descriptive', 'predictive', 'prescriptive', 'limitations'];
            let htmlContent = `
                <div style="border-bottom: 1px solid #27272a; padding-bottom: 16px; margin-bottom: 32px;">
                    <div style="color: #10b981; font-size: 24px; font-weight: bold; letter-spacing: 2px;">NOVAFLOW INTELLIGENCE</div>
                    <div style="color: #71717a; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; margin-top: 4px;">Generated Executive Strategy Brief</div>
                </div>
            `;

            keys.forEach(key => {
                const text = parsedData?.[key] || `Awaiting ${key} analysis...`;
                htmlContent += `
                    <div style="margin-bottom: 24px;">
                        <div style="color: #34d399; font-size: 11px; text-transform: uppercase; letter-spacing: 2px; font-weight: bold; margin-bottom: 8px;">[ ${key.toUpperCase()} ]</div>
                        <div style="border-left: 2px solid rgba(16,185,129,0.3); padding-left: 16px;">
                            <div style="color: #a1a1aa; font-size: 13px; line-height: 1.6; white-space: pre-wrap;">${text}</div>
                        </div>
                    </div>
                `;
            });

            container.innerHTML = htmlContent;
            document.body.appendChild(container);

            // 3. Take a high-res photo of the synthetic dark mode container
            const canvas = await html2canvas(container, {
                scale: 2,
                backgroundColor: '#09090b',
                logging: false
            });

            document.body.removeChild(container);

            // 4. Wrap it in a PDF and instantly download
            const imgData = canvas.toDataURL('image/png');
            const pdf = new jsPDF({ orientation: 'portrait', unit: 'mm', format: 'a4' });
            const pdfWidth = pdf.internal.pageSize.getWidth();
            const pdfHeight = (canvas.height * pdfWidth) / canvas.width;

            pdf.addImage(imgData, 'PNG', 0, 0, pdfWidth, pdfHeight);
            pdf.save(`NovaFlow_Executive_Brief_${new Date().getTime()}.pdf`);

        } catch (err) {
            console.error("PDF generation failed:", err);
        } finally {
            setIsExporting(false);
        }
    };
    const handlePlayAudio = () => {
        if (voiceActive) {
            if (audioRef.current) {
                audioRef.current.pause();
                audioRef.current.currentTime = 0;
            }
            if (setVoiceActive) setVoiceActive(false);
            return;
        }

        if (!audioUrl) {
            console.error("No AWS Audio URL detected.");
            return;
        }

        // Automatically minimize chat panel so the voice agent has free visual space
        if (setIsChatOpen) setIsChatOpen(false);

        const audio = new Audio(audioUrl);
        audioRef.current = audio;
        if (playSFX) playSFX('ai_speak');

        audio.onplay = () => { if (setVoiceActive) setVoiceActive(true); };
        audio.onended = () => { if (setVoiceActive) setVoiceActive(false); };
        audio.onerror = () => { if (setVoiceActive) setVoiceActive(false); };

        audio.play().catch(err => {
            console.error("Audio playback failed:", err);
            if (setVoiceActive) setVoiceActive(false);
        });
    };

    return (
        <div className="flex flex-col gap-6 w-full max-w-4xl relative pb-12">
            <div id="strategy-pdf-content" className="w-full flex flex-col gap-6">
                <div className="hidden print-header">
                    <div className="print-title">NOVAFLOW INTELLIGENCE</div>
                    <div className="print-sub">Generated Executive Strategy Brief</div>
                </div>
                {['diagnostic', 'descriptive', 'predictive', 'prescriptive', 'limitations'].map((key) => (
                    <div key={key}>
                        <div className="text-emerald-400 font-mono text-xs uppercase tracking-[0.2em] mb-2 font-bold">
                            [ {key} ]
                        </div>
                        <div className="border-l-2 border-emerald-500/30 pl-4 mb-2">
                            <div className="text-zinc-400 text-sm">
                                {aiAnalysis?.strategy_brief?.[key] || `Awaiting ${key} analysis...`}
                            </div>
                        </div>
                    </div>
                ))}
            </div>

            <div className="mt-8 flex flex-col md:flex-row justify-center gap-4 w-full">
                <button onClick={generatePDF} className="flex items-center gap-2 px-5 py-2.5 rounded-[2px] border border-white/5 bg-zinc-900 text-zinc-400 font-mono text-[10px] uppercase tracking-widest hover:text-emerald-400 hover:border-emerald-500/30 transition-all">
                    <FileText className="w-4 h-4" /> Export Report PDF
                </button>
                <button
                    onClick={handlePlayAudio}
                    className={`flex items-center gap-2 px-5 py-2.5 rounded-[2px] border font-mono text-[10px] uppercase tracking-widest transition-all duration-300 ${voiceActive ? 'bg-emerald-500/20 border-emerald-500/50 text-emerald-400 shadow-[0_0_15px_rgba(16,185,129,0.3)]' : 'bg-zinc-900 border-white/5 text-zinc-500 hover:text-emerald-400 hover:border-emerald-500/30'}`}
                >
                    {voiceActive ? (
                        <><Volume2 className="w-4 h-4 animate-pulse" /> Transmitting...</>
                    ) : (
                        <><Mic className="w-4 h-4" /> Initialize Audio Brief</>
                    )}
                </button>
            </div>
        </div>
    );
};

// Threaded visualizer component
const VisualizerArtifact = ({ analysisTurns, voiceActive, setVoiceActive, isSignedIn }) => {
    const [activeChartAudio, setActiveChartAudio] = useState(null);

    useEffect(() => {
        return () => window.speechSynthesis.cancel();
    }, []);

    const playChartAudio = (text, chartIndex) => {
        if (voiceActive && activeChartAudio === chartIndex) {
            window.speechSynthesis.cancel();
            setVoiceActive(false);
            setActiveChartAudio(null);
            return;
        }
        // Instantly pause any AWS Audio elements playing in the background
        document.querySelectorAll('audio').forEach(a => a.pause());
        window.speechSynthesis.cancel();

        const utterance = new SpeechSynthesisUtterance(text);
        utterance.pitch = 0.9;
        utterance.rate = 0.95;
        const voices = window.speechSynthesis.getVoices();
        const preferredVoice = voices.find(v => v.name.includes('Google') || v.name.includes('Siri') || v.name.includes('English'));
        if (preferredVoice) utterance.voice = preferredVoice;

        utterance.onstart = () => { setVoiceActive(true); setActiveChartAudio(chartIndex); };
        utterance.onend = () => { setVoiceActive(false); setActiveChartAudio(null); };
        utterance.onerror = () => { setVoiceActive(false); setActiveChartAudio(null); };

        window.speechSynthesis.speak(utterance);
    };

    if (!analysisTurns || analysisTurns.length === 0) {
        return (
            <div className="w-full h-full flex items-center justify-center text-zinc-600 font-mono text-xs uppercase tracking-widest">
                Awaiting Data Execution...
            </div>
        )
    }

    return (
        <motion.div
            initial={{ opacity: 0, y: 15, scale: 0.98 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            transition={{ duration: 0.3, ease: 'easeOut' }}
            className="w-full h-full flex-1 border-0 rounded-none bg-transparent flex flex-col relative z-10"
        >
            <div className="w-full flex justify-center sticky top-0 bg-[#0a0a0a]/90 backdrop-blur z-20 border-b border-white/5 px-6 py-4">
                <div className="flex items-center justify-between w-full max-w-6xl">
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

            <div className="p-6 sm:p-10 flex flex-col flex-1 overflow-y-auto custom-scrollbar w-full items-center">
                <div className="w-full max-w-6xl">

                    {/* The Continuous Glowing Pipeline (Noodle) */}
                    <div className="relative border-l-2 border-emerald-500/20 ml-2 sm:ml-6 space-y-16 pb-12">

                        {analysisTurns.map((turn, turnIndex) => (
                            <div key={turn.id} className="relative w-full">

                                {/* 1. The Prompt Node (Anchor) */}
                                <div className="absolute -left-[11px] top-6 w-5 h-5 rounded-full bg-zinc-950 border border-emerald-500/50 flex items-center justify-center shadow-[0_0_15px_rgba(16,185,129,0.4)] z-10">
                                    <div className="w-1.5 h-1.5 bg-emerald-400 rounded-full animate-pulse"></div>
                                </div>

                                <div className="ml-8 sm:ml-12 mb-10 w-full max-w-4xl">
                                    <div className="bg-zinc-900/60 border border-emerald-500/20 rounded-lg p-5 shadow-lg relative group transition-all duration-300">
                                        <div className="text-[10px] text-emerald-400 font-mono uppercase tracking-widest mb-3 flex items-center gap-2">
                                            <Terminal className="w-3.5 h-3.5" /> User Command Executed
                                        </div>
                                        {/* Auto-clamp massive prompts but allow expansion on hover */}
                                        <div className="text-sm text-zinc-300 font-sans leading-relaxed line-clamp-3 group-hover:line-clamp-none transition-all duration-300 whitespace-pre-wrap">
                                            {turn.prompt || "Historical Analysis Command"}
                                        </div>
                                    </div>
                                </div>

                                {/* 2. The Result Nodes (Charts & Text) attached to the main line */}
                                <div className="ml-8 sm:ml-12 flex flex-col gap-10">

                                    {/* Map over charts instead of text points so no graph gets left behind */}
                                    {(turn.charts || []).map((chart, chartIndex) => {

                                        // Find the matching text for this chart, or fallback safely
                                        const point = turn.point_analyses?.find(p => p.point_id === chart.meta?.scenario_id) || turn.point_analyses?.[chartIndex] || {};

                                        let darkLayout = null;
                                        if (chart && chart.layout) {
                                            darkLayout = {
                                                ...chart.layout,
                                                title: '',
                                                autosize: true,
                                                paper_bgcolor: 'transparent',
                                                plot_bgcolor: 'transparent',
                                                font: { family: 'monospace', color: '#a1a1aa' },
                                                xaxis: { ...chart.layout?.xaxis, gridcolor: '#1f2937', zerolinecolor: '#374151' },
                                                yaxis: { ...chart.layout?.yaxis, gridcolor: '#1f2937', zerolinecolor: '#374151' },
                                                margin: { t: 20, r: 20, l: 40, b: 40 }
                                            };
                                        }

                                        const isThisSpeaking = activeChartAudio === chartIndex;

                                        return (
                                            <div key={chartIndex} className="relative w-full bg-zinc-950 border border-zinc-800 rounded-xl overflow-hidden shadow-2xl">

                                                {/* The horizontal branch noodle connecting to the card */}
                                                <div className="absolute -left-[48px] top-8 w-[48px] h-[2px] bg-emerald-500/20 hidden sm:block"></div>

                                                {/* Card Header Strip */}
                                                <div className="w-full flex items-center justify-between p-4 bg-zinc-900/50 border-b border-zinc-800">
                                                    <div className="flex items-center gap-4">
                                                        <div className="w-6 h-6 rounded bg-emerald-500/10 text-emerald-400 flex items-center justify-center font-mono text-xs font-bold border border-emerald-500/30 shrink-0">
                                                            {chartIndex + 1}
                                                        </div>
                                                        <h3 className="text-sm font-semibold text-zinc-100 tracking-tight">
                                                            {point.point_title || chart.layout?.title?.text || `Analysis Node ${chartIndex + 1}`}
                                                        </h3>
                                                    </div>
                                                    <button
                                                        disabled={!isSignedIn}
                                                        onClick={() => playChartAudio(Array.isArray(point.point_answers) ? point.point_answers.join(' ') : point.point_answers, chartIndex)}
                                                        className={`flex items-center gap-2 px-3 py-1.5 rounded-[2px] border font-mono text-[9px] uppercase tracking-widest transition-all duration-300 disabled:opacity-50 disabled:cursor-not-allowed ${isThisSpeaking ? 'bg-emerald-500/20 border-emerald-500/50 text-emerald-400 shadow-[0_0_15px_rgba(16,185,129,0.3)]' : 'bg-black/40 border-white/5 text-zinc-500 hover:text-emerald-400 hover:border-emerald-500/30'}`}
                                                    >
                                                        {isThisSpeaking ? (
                                                            <><Volume2 className="w-3 h-3 animate-pulse" /> Transmitting...</>
                                                        ) : (
                                                            !isSignedIn ? <><Lock className="w-3 h-3" /> Audio Node</> : <><Mic className="w-3 h-3" /> Audio Node</>
                                                        )}
                                                    </button>
                                                </div>

                                                {/* Split Content: Text on left, Graph on right */}
                                                <div className="w-full flex flex-col md:flex-row gap-0 bg-black/40">
                                                    <div className="w-full md:w-1/3 flex flex-col max-h-[400px] overflow-y-auto custom-scrollbar pr-4 p-6 sm:p-8 border-r border-zinc-800">
                                                        <div className="text-sm text-zinc-300 leading-relaxed prose prose-invert prose-emerald max-w-none">
                                                            <ReactMarkdown>
                                                                {Array.isArray(point.point_answers)
                                                                    ? point.point_answers.join('\n')
                                                                    : String(point.point_answers || "Synthesizing visual insights...")}
                                                            </ReactMarkdown>
                                                        </div>
                                                    </div>
                                                    <div className="w-full md:w-2/3 min-w-0 h-[400px] relative bg-black/20 rounded-lg p-2 overflow-hidden">
                                                        {!chart.error ? (
                                                            <Plot
                                                                data={chart.data}
                                                                layout={darkLayout}
                                                                useResizeHandler={true}
                                                                style={{ width: '100%', height: '100%' }}
                                                                config={{ displayModeBar: true, displaylogo: false }}
                                                            />
                                                        ) : (
                                                            <div className="flex-1 w-full h-full flex flex-col items-center justify-center text-red-500 font-mono text-[10px] uppercase tracking-widest border border-dashed border-red-900/50 rounded-lg p-8">
                                                                <Activity className="w-6 h-6 mb-3 opacity-50" />
                                                                Error: {chart.error}
                                                            </div>
                                                        )}
                                                    </div>
                                                </div>
                                            </div>
                                        )
                                    })}
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            </div>
        </motion.div>
    );
};


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

const UserChatBubble = ({ message }) => {
    const [isExpanded, setIsExpanded] = useState(false);
    const shouldTruncate = message.length > 200;
    const displayText = shouldTruncate && !isExpanded ? message.slice(0, 200) + "..." : message;

    return (
        <div className="bg-zinc-800/80 text-zinc-200 px-4 py-2 text-sm border border-white/5 max-w-[90%] rounded-[2px] flex flex-col items-start transition-all">
            <div className="whitespace-pre-wrap">{displayText}</div>
            {shouldTruncate && (
                <button
                    onClick={() => setIsExpanded(!isExpanded)}
                    className="text-[10px] text-zinc-500 hover:text-emerald-400 mt-2 font-mono uppercase tracking-widest transition-colors flex items-center gap-1"
                >
                    {isExpanded ? "Show Less" : "Show More"}
                </button>
            )}
        </div>
    );
};

// --- Main App ---

export default function NovaFlowDashboard() {
    const [isListening, setIsListening] = useState(false);
    const [isInputExpanded, setIsInputExpanded] = useState(false);
    const [file, setFile] = useState(null);
    const [query, setQuery] = useState('');
    const textareaRef = useRef(null);
    const recognitionRef = useRef(null);
    const isListeningRef = useRef(false);
    const baseQueryRef = useRef("");
    const silenceTimerRef = useRef(null);

    const stopListening = () => {
        if (recognitionRef.current) recognitionRef.current.stop();
        if (silenceTimerRef.current) clearTimeout(silenceTimerRef.current);
        isListeningRef.current = false;
        setIsListening(false);
        playSFX('deactivate');
    };

    const toggleListening = () => {
        const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
        if (!SpeechRecognition) {
            alert("Speech Recognition is not supported. Please use Chrome/Edge.");
            return;
        }

        // TURN OFF
        if (isListeningRef.current) {
            stopListening();
            return;
        }

        // TURN ON
        playSFX('activate');
        isListeningRef.current = true;
        setIsListening(true);

        // Freeze whatever text is currently in the box
        baseQueryRef.current = query;

        const recognition = new SpeechRecognition();
        recognitionRef.current = recognition;
        recognition.continuous = true;
        recognition.interimResults = true;

        recognition.onresult = (event) => {
            const sessionTranscript = Array.from(event.results)
                .map(res => res[0].transcript)
                .join('');

            setQuery((baseQueryRef.current ? baseQueryRef.current + ' ' : '') + sessionTranscript);

            // Reset the 5-second silence auto-shutoff timer
            if (silenceTimerRef.current) clearTimeout(silenceTimerRef.current);
            silenceTimerRef.current = setTimeout(stopListening, 5000);
        };

        recognition.onend = () => {
            // Let the browser naturally close. The silence timer handles cleanup.
            if (isListeningRef.current) {
                setIsListening(false);
                isListeningRef.current = false;
            }
        };

        recognition.onerror = (e) => {
            console.error("Mic Error:", e.error);
            if (e.error === 'not-allowed') {
                alert("Microphone access denied! Please click the lock icon in your URL bar and allow microphone access.");
                stopListening();
            }
        };

        // Start the initial 5-second timer in case user never speaks
        silenceTimerRef.current = setTimeout(stopListening, 5000);
        recognition.start();
    };

    useEffect(() => {
        if (textareaRef.current) {
            if (isInputExpanded) {
                textareaRef.current.style.height = '100%';
            } else {
                textareaRef.current.style.height = 'auto';
                textareaRef.current.style.height = textareaRef.current.scrollHeight + 'px';
            }
        }
    }, [query, isInputExpanded]);

    const today = new Date().toISOString().split('T')[0];
    const quotaKey = `novaflow_quota_${today}`;
    const [questionsAsked, setQuestionsAsked] = useState(() => {
        const stored = localStorage.getItem(quotaKey);
        return stored ? parseInt(stored, 10) : 0;
    });

    const [showPaywall, setShowPaywall] = useState(false);
    const [chatHistory, setChatHistory] = useState([]);
    const [isChatOpen, setIsChatOpen] = useState(true);
    const [processing, setProcessing] = useState(false);
    const [rightContent, setRightContent] = useState('empty');
    const [activeTab, setActiveTab] = useState('data');
    const [isHistoryOpen, setIsHistoryOpen] = useState(false);

    // Backend Execution State
    const [currentTaskId, setCurrentTaskId] = useState('');
    const lastPromptRef = useRef(""); // Crucial for threading
    const [currentSessionId, setCurrentSessionId] = useState(null);
    const [activeFileKey, setActiveFileKey] = useState(null);

    // The New Threaded Pipeline State
    const [analysisTurns, setAnalysisTurns] = useState([]);

    // We keep aiAnalysisText to power the other tabs (Strategy, Logs)
    const [aiAnalysisText, setAiAnalysisText] = useState({});

    // Real CSV State
    const [csvHeaders, setCsvHeaders] = useState([]);
    const [csvPreviewData, setCsvPreviewData] = useState([]);

    // History API State
    const [sessionHistory, setSessionHistory] = useState([]);

    // PLG State
    const { user, isLoaded, isSignedIn } = useUser();
    const [userEmail, setUserEmail] = useState(() => {
        return localStorage.getItem('novaflow_user_email') || null;
    });
    const [voiceActive, setVoiceActive] = useState(false);

    useEffect(() => {
        if (isSignedIn && user?.primaryEmailAddress?.emailAddress) {
            setUserEmail(user.primaryEmailAddress.emailAddress);
        } else if (isSignedIn === false) {
            setUserEmail(null);
            localStorage.removeItem('novaflow_user_email');
            setQuestionsAsked(0);
        }
    }, [isSignedIn, user]);

    const [mobilePanelOpen, setMobilePanelOpen] = useState(false);

    const [userId] = useState(() => {
        let storedId = localStorage.getItem('novaflow_guest_id');

        if (!storedId) {
            const match = document.cookie.match(/(?:^|; )novaflow_guest_id=([^;]*)/);
            if (match) storedId = match[1];
        }

        if (!storedId) {
            storedId = 'guest_' + Math.random().toString(36).substring(2, 9);
        }

        localStorage.setItem('novaflow_guest_id', storedId);
        document.cookie = `novaflow_guest_id=${storedId}; max-age=${60 * 60 * 24 * 365}; path=/`;

        return storedId;
    });

    useEffect(() => {
        localStorage.setItem(quotaKey, questionsAsked.toString());
    }, [questionsAsked, quotaKey]);

    useEffect(() => {
        if (userEmail) {
            localStorage.setItem('novaflow_user_email', userEmail);
        } else {
            localStorage.removeItem('novaflow_user_email');
        }
    }, [userEmail]);

    const fetchHistory = async () => {
        try {
            const response = await fetch(import.meta.env.VITE_AWS_API_URL, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ action: 'get_history', user_id: user?.id || userId })
            });

            if (response.ok) {
                const data = await response.json();

                const safeDate = (dateStr) => {
                    if (!dateStr) return new Date().toISOString();
                    const d = new Date(dateStr);
                    return isNaN(d.getTime()) ? new Date().toISOString() : d.toISOString();
                };

                setSessionHistory(prev => {
                    const normalizedHistory = (data.history || []).map(h => {
                        // 1. DIRECT CONNECTION: Grab the exact title from DynamoDB
                        let finalTitle = h.session_title;
                        const promptText = h.prompt_snippet || h.prompt || 'Historical Analysis';

                        // 2. PURE FALLBACK: ONLY if DynamoDB is literally empty, grab 4 words of the prompt
                        if (!finalTitle || finalTitle.trim() === '') {
                            let cleanPrompt = promptText.replace(/^(To:|From:|Subject:|Executive Memo:)\s*/gi, '').trim();
                            finalTitle = cleanPrompt ? cleanPrompt.split(' ').slice(0, 4).join(' ') + '...' : "Analysis Session";
                        }

                        return {
                            ...h,
                            timestamp: safeDate(h.timestamp || h.last_updated),
                            prompt_snippet: promptText,
                            session_title: finalTitle
                        };
                    });

                    const dbIds = new Set(normalizedHistory.map(h => h.task_id));
                    const localOnly = prev.filter(h => !dbIds.has(h.task_id));

                    // Force chronological sort right here to prevent UI jumping
                    const combined = [...localOnly, ...normalizedHistory];
                    combined.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));

                    return combined;
                });
            }
        } catch (err) {
            console.error("Failed to load history:", err);
        }
    };

    useEffect(() => {
        if (user?.id || userId) {
            fetchHistory();
        }
    }, [user?.id, userId]);

    if (!isLoaded) {
        return <div className="h-screen w-screen bg-zinc-950 flex items-center justify-center text-zinc-500 font-mono text-sm">Initializing Secure Workspace...</div>;
    }

    const MAX_ANON = 3;
    const MAX_LEAD = 15;
    const isGodMode = userEmail === import.meta.env.VITE_ADMIN_EMAIL;
    const quotaLimit = isGodMode ? 9999 : (userEmail ? MAX_LEAD : MAX_ANON);

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

                    // Reset pipeline for new file
                    setAnalysisTurns([]);
                    setAiAnalysisText({});
                    setCurrentSessionId('sess_' + Date.now());
                    setActiveFileKey(null); // Reset the key for the new file
                }
            };
            const slice = uploadedFile.slice(0, 500 * 1024);
            reader.readAsText(slice);
        }
    };

    const handleAsk = async () => {
        if (!query.trim() || !file || processing) return;

        if (questionsAsked >= quotaLimit) {
            setShowPaywall(true);
            return;
        }

        const currentQuery = query;
        lastPromptRef.current = currentQuery; // Store for the UI thread

        setChatHistory(prev => [...prev, { role: 'user', content: currentQuery }]);
        setQuery('');
        const textarea = document.getElementById('chat-textarea');
        if (textarea) textarea.style.height = 'auto';
        setQuestionsAsked(prev => prev + 1);
        setCurrentTaskId(null);
        setProcessing(true);
        setRightContent('processing');
        setActiveTab('logs');

        if (window.innerWidth < 768) {
            setMobilePanelOpen(false);
        }

        try {
            let file_key_to_use = activeFileKey;

            if (!activeFileKey) {
                const uniqueSessionName = `${Date.now()}_${file.name}`;
                const urlResponse = await fetch(import.meta.env.VITE_AWS_API_URL, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ action: 'get_upload_url', file_name: uniqueSessionName })
                });

                if (urlResponse.status === 403) { setProcessing(false); return; }
                if (urlResponse.status === 402) { setProcessing(false); setShowPaywall(true); return; }
                if (!urlResponse.ok) throw new Error(`HTTP error! status: ${urlResponse.status}`);

                const urlData = await urlResponse.json();
                const { upload_url, file_key } = urlData;

                const uploadResponse = await fetch(upload_url, { method: 'PUT', body: file });
                if (!uploadResponse.ok) throw new Error(`Upload failed! status: ${uploadResponse.status}`);

                setActiveFileKey(file_key);
                file_key_to_use = file_key;
            }

            // Execution Call WITH Memory passed to Backend
            const execResponse = await fetch(import.meta.env.VITE_AWS_API_URL, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    action: 'execute',
                    user_id: user?.id || userId,
                    file_key: file_key_to_use,
                    prompt: currentQuery,
                    email: user?.primaryEmailAddress?.emailAddress,
                    chat_history: chatHistory // The Memory Fix Sent to Nova
                })
            });

            if (execResponse.status === 403) { setProcessing(false); return; }
            if (execResponse.status === 402) { setProcessing(false); setShowPaywall(true); return; }
            if (!execResponse.ok) throw new Error(`HTTP error! status: ${execResponse.status}`);

            const data = await execResponse.json();
            setCurrentTaskId(data.task_id);

        } catch (error) {
            console.error("Connection error:", error);
            setChatHistory(prev => [...prev, { role: 'ai', content: `> ERR_CONNECTION: ${error.message}` }]);
            setProcessing(false);
            setRightContent('empty');
        }
    };

    const onProcessingComplete = (analysisRaw, chartRaw, error) => {
        if (error) {
            setChatHistory(prev => [...prev, { role: 'ai', content: '> Analysis complete. Artifact and insights ready.' }]);
            const tempTitle = "Pipeline Failed";
            setSessionHistory(prev => [{
                task_id: currentTaskId,
                timestamp: new Date().toISOString(),
                prompt_snippet: lastPromptRef.current,
                session_title: tempTitle,
                display_title: tempTitle
            }, ...prev]);
            setProcessing(false);
            setRightContent('table');
            return;
        }
        try {
            const parsedAnalysis = typeof analysisRaw === 'string' ? JSON.parse(analysisRaw) : (analysisRaw || {});
            const parsedCharts = typeof chartRaw === 'string' ? JSON.parse(chartRaw) : (chartRaw || []);

            setAnalysisTurns(prev => [
                ...prev,
                {
                    id: Date.now(),
                    prompt: lastPromptRef.current,
                    strategy_brief: parsedAnalysis.strategy_brief || {},
                    point_analyses: parsedAnalysis.point_analyses || [],
                    charts: parsedCharts
                }
            ]);

            setAiAnalysisText(prev => ({
                strategy_brief: parsedAnalysis.strategy_brief || prev.strategy_brief,
                raw_sql: parsedAnalysis.raw_sql,
                preprocessing_log: parsedAnalysis.preprocessing_log,
                audio_url: parsedAnalysis.audio_url
            }));

            if (playSFX) playSFX('success');

            // SMART LOCAL TITLE: Strips multiline headers properly
            let cleanLocalPrompt = lastPromptRef.current.replace(/(To:|From:|Subject:|Executive Memo:).*?\n/gi, '').trim();
            const tempTitle = cleanLocalPrompt ? cleanLocalPrompt.split(' ').slice(0, 4).join(' ') + "..." : "Processing...";

            setSessionHistory(prev => {
                if (prev.some(p => p.task_id === currentTaskId)) return prev;
                return [{
                    task_id: currentTaskId,
                    timestamp: new Date().toISOString(),
                    prompt_snippet: lastPromptRef.current,
                    session_title: tempTitle,
                    display_title: tempTitle
                }, ...prev];
            });
            setChatHistory(prev => [...prev, { role: 'ai', content: '> Analysis complete. Artifact and insights ready.' }]);

        } catch (err) {
            console.error("Parse error:", err);
            setChatHistory(prev => [...prev, { role: 'ai', content: '> Parsing Error.' }]);
        } finally {
            setProcessing(false);
            setRightContent('chart');
            setActiveTab('viz');

            // 1st Check: 2 seconds (Fast AWS execution)
            setTimeout(() => {
                if (user?.id || userId) fetchHistory();
            }, 2000);

            // 2nd Check: 8 seconds (Failsafe for when Nova Sonic audio takes a long time)
            setTimeout(() => {
                if (user?.id || userId) fetchHistory();
            }, 8000);
        }
    };

    const categorizeHistory = (historyList) => {
        const groups = { 'Today': [], 'Previous 7 Days': [], 'Older': [] };
        const today = new Date(); today.setHours(0, 0, 0, 0);
        const titleCounts = {};

        // THE FIX: Force strict chronological sorting (Newest to Oldest)
        const sortedList = [...historyList].sort((a, b) => {
            const dateA = new Date(a.timestamp || 0);
            const dateB = new Date(b.timestamp || 0);
            return dateB - dateA;
        });

        // First pass: Deduplicate titles (iterate oldest to newest to count up)
        const processedList = sortedList.reverse().map(item => {
            const baseTitle = item.session_title || item.prompt_snippet;
            if (titleCounts[baseTitle]) {
                const newCount = titleCounts[baseTitle] + 1;
                titleCounts[baseTitle] = newCount;
                item.display_title = `${baseTitle} (${newCount})`;
            } else {
                titleCounts[baseTitle] = 1;
                item.display_title = baseTitle;
            }
            return item;
        }).reverse(); // Flip back to newest first

        // Second pass: Group by date
        processedList.forEach(item => {
            const itemDate = new Date(item.timestamp);
            const diffDays = Math.ceil(Math.abs(today - itemDate) / (1000 * 60 * 60 * 24));
            if (itemDate >= today) groups['Today'].push(item);
            else if (diffDays <= 7) groups['Previous 7 Days'].push(item);
            else groups['Older'].push(item);
        });
        return groups;
    };
    const groupedHistory = categorizeHistory(sessionHistory);

    const resetWorkspace = () => {
        setChatHistory([{ role: 'ai', content: `> Dataset mounted successfully. Schema inference complete. Ready for new analysis.` }]);
        setQuery('');
        setCurrentTaskId('');
        setAnalysisTurns([]); // Reset Thread UI
        setAiAnalysisText({});
        setRightContent('table');
        setActiveTab('data');
        setProcessing(false); // CRITICAL: This removes SYSTEM_LOCKED
        setIsChatOpen(true);
        setIsHistoryOpen(true);
        if (window.innerWidth < 768) setIsHistoryOpen(false);
    };

    const deleteSession = async (e, taskId) => {
        e.stopPropagation(); // Prevents the click from loading the session

        // 1. OPTIMISTIC UI: Instantly remove the chat from the sidebar so the user doesn't wait
        setSessionHistory(prev => prev.filter(session => session.task_id !== taskId));

        // 2. If they are deleting the chat currently open on the main screen, clear the screen
        if (currentTaskId === taskId) {
            resetWorkspace();
        }

        // 3. Send the kill command to AWS silently in the background
        try {
            await fetch(import.meta.env.VITE_AWS_API_URL, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ action: 'delete_task', task_id: taskId, user_id: user?.id || userId })
            });
        } catch (err) {
            console.error("Deletion failed:", err);
        }
    };

    const loadHistoricalSession = (session) => {
        setAnalysisTurns([]); // Wipe canvas
        setAiAnalysisText({}); // Wipe analysis
        setChatHistory([{ role: 'ai', content: `> Mounted Historical Context: ${session.prompt_snippet}` }]);
        setCurrentTaskId(session.task_id);
        setProcessing(true);
        setRightContent('processing');
        setActiveTab('logs');
        if (window.innerWidth < 768) setIsHistoryOpen(false);
    };

    return (
        <div className="w-screen h-screen bg-[#050505] text-white font-sans flex flex-col overflow-hidden selection:bg-emerald-500/30">
            {/* Background Base */}
            <div className="absolute inset-0 z-0 bg-grid-pattern opacity-[0.03] pointer-events-none" />
            <div className="relative z-10 flex w-full flex-1 overflow-hidden flex-col md:flex-row">

                {/* Mobile Header */}
                <div className="md:hidden flex items-center justify-between p-4 border-b border-white/5 bg-zinc-900/50 z-30 shadow-md">
                    <div className="flex items-center gap-3">
                        {/* THE NEW NOVAFLOW LOGO */}
                        <div className="flex items-center justify-center w-10 h-10 shrink-0">
                            <img
                                src="/novaflow.svg"
                                alt="NovaFlow Logo"
                                className="w-8 h-8 drop-shadow-[0_0_12_rgba(16,185,129,0.4)] transition-transform hover:scale-105"
                            />
                        </div>
                        <h1 className="text-lg font-semibold tracking-tight text-zinc-100 font-sans">NovaFlow</h1>
                    </div>
                    <div className={`text-xs font-mono px-2 py-1.5 border rounded-[2px] transition-colors flex flex-col items-center flex-shrink-0 ${questionsAsked >= quotaLimit ? "bg-red-500/5 text-red-500 border-red-500/20" : "bg-white/[0.02] text-zinc-400 border-white/5"}`}>
                        <span className="text-[8px] uppercase tracking-widest text-zinc-600 mb-0.5">COMPUTE QUOTA</span>
                        <span className="text-[11px] font-bold">{Math.max(0, quotaLimit - questionsAsked)}/{quotaLimit} REQ</span>
                    </div>
                </div>

                {/* Far-Left Activity Rail */}
                <div className="hidden md:flex w-[50px] shrink-0 border-r border-white/5 bg-black flex-col items-center py-4 justify-between z-20 shadow-[1px_0_10px_rgba(0,0,0,0.8)]">
                    {/* THE NEW NOVAFLOW LOGO */}
                    <div className="flex items-center justify-center w-10 h-10 shrink-0">
                        <img
                            src="/novaflow.svg"
                            alt="NovaFlow Logo"
                            className="w-8 h-8 drop-shadow-[0_0_12px_rgba(16,185,129,0.4)] transition-transform hover:scale-105"
                        />
                    </div>
                    <div className="flex flex-col gap-6 items-center flex-1 mt-8 text-zinc-600">
                        <div onClick={() => setIsChatOpen(!isChatOpen)} className="p-2 hover:bg-white/10 hover:text-zinc-300 rounded-[2px] cursor-pointer transition-colors" title="Toggle Command Center"><LayoutPanelLeft className="w-5 h-5" strokeWidth={1.5} /></div>
                        <div onClick={() => setIsHistoryOpen(!isHistoryOpen)} className={`p-2 rounded-[2px] cursor-pointer block transition-colors ${isHistoryOpen ? 'bg-white/10 text-zinc-200 drop-shadow-[0_0_8px_rgba(255,255,255,0.2)]' : 'hover:bg-white/10 hover:text-zinc-300'}`} title="Workspace History"><MessageSquare className="w-5 h-5" strokeWidth={1.5} /></div>
                        <div className="p-2 hover:text-zinc-300 rounded-[2px] cursor-pointer"><Activity className="w-5 h-5" strokeWidth={1.5} /></div>
                        <div className="p-2 hover:text-zinc-300 rounded-[2px] cursor-pointer"><Settings className="w-5 h-5" strokeWidth={1.5} /></div>
                    </div>
                    <div className="flex items-center justify-center mt-auto">
                        <SignedOut>
                            <SignInButton mode="modal">
                                <div className="w-8 h-8 rounded-full bg-zinc-900 border border-white/10 flex items-center justify-center cursor-pointer hover:border-emerald-500/50 hover:bg-emerald-500/10 transition-colors group" title="Sign In">
                                    <User className="w-4 h-4 text-zinc-500 group-hover:text-emerald-400 transition-colors" />
                                </div>
                            </SignInButton>
                        </SignedOut>
                        <SignedIn>
                            <UserButton appearance={{ elements: { userButtonAvatarBox: "w-8 h-8 border border-white/10 shadow-[0_0_15px_rgba(16,185,129,0.2)]" } }} />
                        </SignedIn>
                    </div>
                </div>

                {/* History Sidebar */}
                {isHistoryOpen && (
                    <div className="w-[280px] shrink-0 border-r border-white/10 bg-zinc-950 flex flex-col z-20 h-full relative">
                        <div className="p-3 border-b border-white/5 flex justify-between items-center bg-zinc-950/80 shrink-0">
                            <button onClick={resetWorkspace} className="flex-1 mr-2 py-2 px-3 bg-white/[0.03] hover:bg-emerald-500/10 text-zinc-300 hover:text-emerald-400 border border-white/5 hover:border-emerald-500/30 rounded-lg text-[13px] font-medium transition-all flex items-center gap-2">
                                <MessageSquare className="w-4 h-4" /> New Analysis
                            </button>
                            <button onClick={() => setIsHistoryOpen(false)} className="p-2 text-zinc-500 hover:text-white hover:bg-white/5 rounded-lg transition-colors shrink-0">
                                <PanelLeftClose className="w-5 h-5" />
                            </button>
                        </div>
                        <div className="flex-1 overflow-y-auto p-3 space-y-6 custom-scrollbar bg-zinc-950/50">
                            {groupedHistory && Object.entries(groupedHistory).map(([groupName, items]) => {
                                if (!items || items.length === 0) return null;
                                return (
                                    <div key={groupName} className="flex flex-col gap-1">
                                        <div className="px-2 mb-1 text-[10px] font-semibold tracking-wider text-zinc-500 uppercase">{groupName}</div>
                                        {items.map((item, idx) => {
                                            const isActive = currentTaskId === item.task_id;
                                            return (
                                                <button
                                                    key={idx}
                                                    onClick={() => loadHistoricalSession(item)}
                                                    className={`relative w-full text-left px-3 py-2.5 rounded-lg flex items-center gap-3 transition-all group ${isActive ? 'bg-zinc-800/80 text-zinc-100' : 'text-zinc-400 hover:bg-zinc-900/80 hover:text-zinc-200'}`}
                                                >
                                                    <div className="flex items-center justify-between w-full">
                                                        <div className="flex items-center gap-3 w-[85%]">
                                                            <MessageSquare className={`w-4 h-4 shrink-0 mt-1 ${isActive ? 'text-emerald-400' : 'text-zinc-500 group-hover:text-zinc-400'}`} />
                                                            <div className="flex flex-col w-full text-left min-w-0">
                                                                <span className="text-[9px] font-mono tracking-widest uppercase mb-0.5 opacity-80 text-emerald-500/80">
                                                                    {new Date(item.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                                                                </span>

                                                                <div className="truncate text-[13px] font-medium leading-relaxed cursor-default">
                                                                    {item.display_title}
                                                                </div>

                                                                {/* Session details tooltip */}
                                                                <div className="absolute left-0 top-[95%] hidden group-hover:block z-[9999] bg-zinc-950 p-3.5 rounded-md border border-emerald-500/30 whitespace-normal w-max max-w-[280px] shadow-[0_15px_40px_rgba(0,0,0,0.9)] drop-shadow-[0_0_15px_rgba(16,185,129,0.2)] text-left cursor-text" onClick={(e) => e.stopPropagation()}>
                                                                    <div className="text-emerald-400 text-[12px] font-bold tracking-wide mb-1.5 leading-tight">
                                                                        {item.session_title ? item.session_title.replace(/\.\.\.$/, '') : "Analysis Session"}
                                                                    </div>
                                                                    <div className="text-zinc-400 text-[10px] font-mono leading-relaxed line-clamp-3 border-t border-white/5 pt-2">
                                                                        {item.prompt_snippet}
                                                                    </div>
                                                                </div>
                                                            </div>
                                                        </div>

                                                        <div
                                                            onClick={(e) => deleteSession(e, item.task_id)}
                                                            className="opacity-0 group-hover:opacity-100 p-1.5 hover:bg-red-500/20 text-red-500/50 hover:text-red-400 transition-all rounded"
                                                            title="Delete Session"
                                                        >
                                                            <Trash2 className="w-3.5 h-3.5" />
                                                        </div>
                                                    </div>
                                                </button>
                                            );
                                        })}
                                    </div>
                                );
                            })}
                        </div>
                    </div>
                )}

                {mobilePanelOpen && rightContent !== 'empty' && (
                    <div className="md:hidden fixed inset-0 bg-black/60 backdrop-blur-sm z-40" onClick={() => setMobilePanelOpen(false)} />
                )}

                {/* Left Panel (Command Center) */}
                <div className={`flex flex-col bg-zinc-950 border-r border-white/5 h-full transform transition-all duration-500 overflow-hidden ${isChatOpen ? 'w-full md:w-[450px] lg:w-[500px] opacity-100 translate-x-0' : 'w-0 opacity-0 -translate-x-10'}`}>

                    <div className="md:hidden flex items-center justify-between p-4 border-b border-white/5 bg-zinc-900/50">
                        <span className="font-sans font-semibold tracking-tight text-zinc-100">Command Terminal</span>
                        <button onClick={() => setMobilePanelOpen(false)} className="p-1 rounded bg-white/5 text-zinc-400 hover:text-white">
                            <X className="w-4 h-4" />
                        </button>
                    </div>

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

                    {!file ? (
                        <div className="flex-1 flex flex-col items-center justify-center p-8">
                            <div className="w-full h-48 border border-dashed border-white/10 bg-zinc-900/20 flex items-center justify-center flex-col transition-all hover:bg-zinc-900/40 hover:border-emerald-500/30 group relative cursor-pointer">
                                <input type="file" accept=".csv" className="absolute inset-0 w-full h-full opacity-0 cursor-pointer" onChange={handleFileUpload} />
                                <UploadCloud className="w-8 h-8 text-zinc-600 mb-4 group-hover:text-emerald-400/80 transition-colors" strokeWidth={1} />
                                <div className="text-sm font-medium text-zinc-300 mb-1">Inject Data Source</div>
                                <div className="text-[10px] font-mono text-zinc-600 uppercase tracking-widest">CSV / XLSX // MAX 50MB</div>
                            </div>
                        </div>
                    ) : (
                        <div className="flex-1 flex flex-col overflow-hidden relative">
                            <SchemaInspector headers={csvHeaders} />

                            <div className="flex-1 overflow-y-auto p-6 flex flex-col gap-5 bg-black/20 shadow-inner">
                                {chatHistory.map((m, i) => (
                                    <motion.div key={i} initial={{ opacity: 0, x: m.role === 'user' ? 2 : -2 }} animate={{ opacity: 1, x: 0 }} transition={{ duration: 0.2 }} className={`flex w-full ${m.role === 'user' ? 'justify-end' : 'justify-start'}`}>
                                        {m.role === 'user' ? (
                                            <UserChatBubble message={m.content} />
                                        ) : (
                                            <div className="font-mono text-zinc-500 text-[11px] leading-relaxed w-full pl-3 border-l text-shadow-sm border-emerald-500/30 bg-zinc-900/20 py-1">
                                                {m.content}
                                            </div>
                                        )}
                                    </motion.div>
                                ))}
                                <div className="pb-4 shrink-0"></div>
                            </div>

                            <div className="font-mono text-[9px] text-zinc-600 uppercase tracking-widest px-4 pt-3 flex justify-between bg-black">
                                <span>Terminal Input</span>
                                <span className="text-emerald-500/50">Secure</span>
                            </div>
                            <div className={`p-3 bg-black relative ${isInputExpanded ? 'z-[100]' : 'z-20'}`}>
                                {isInputExpanded && <div className="fixed inset-0 z-[90] bg-zinc-950/80 backdrop-blur-sm" onClick={() => setIsInputExpanded(false)}></div>}
                                <div className={isInputExpanded
                                    ? "fixed top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[95vw] md:w-full max-w-4xl h-[80vh] bg-zinc-950 border border-emerald-500/50 rounded-lg flex flex-col p-4 md:p-8 shadow-[0_0_50px_rgba(16,185,129,0.1)] z-[100]"
                                    : "relative group bg-zinc-900/60 border border-white/10 focus-within:border-emerald-500/50 focus-within:bg-zinc-900 transition-all rounded-[2px]"}>

                                    {isInputExpanded && (
                                        <div className="flex justify-between items-center mb-4 border-b border-white/10 pb-4 shrink-0">
                                            <span className="text-emerald-500 font-mono text-xs uppercase tracking-widest flex items-center gap-2">
                                                <Maximize2 className="w-4 h-4" /> Focus Mode
                                            </span>
                                            <button onClick={() => setIsInputExpanded(false)} className="text-zinc-500 hover:text-white transition-colors bg-white/5 p-1 rounded">
                                                <Minimize2 className="w-4 h-4" />
                                            </button>
                                        </div>
                                    )}

                                    {!isInputExpanded && <ChevronRight className={`absolute top-4 left-3 w-4 h-4 transition-colors ${processing ? 'text-zinc-700' : 'text-emerald-500'}`} />}

                                    <textarea
                                        id="chat-textarea"
                                        ref={textareaRef}
                                        disabled={processing}
                                        autoFocus
                                        className={isInputExpanded
                                            ? "flex-1 w-full bg-transparent text-white resize-none outline-none font-mono text-[13px] leading-relaxed overflow-y-auto custom-scrollbar"
                                            : "w-full bg-transparent resize-none outline-none text-white text-[13px] min-h-[3rem] max-h-[30vh] overflow-y-auto py-3 pl-9 pr-[70px] font-mono disabled:opacity-50 block placeholder-zinc-700"}
                                        placeholder={isInputExpanded ? "Enter your advanced logic here..." : (processing ? "SYSTEM_LOCKED..." : "execute command...")}
                                        value={query}
                                        onChange={(e) => setQuery(e.target.value)}
                                        onKeyDown={(e) => {
                                            if (e.key === 'Enter' && !e.shiftKey) {
                                                e.preventDefault();
                                                handleAsk();
                                                if (isInputExpanded) setIsInputExpanded(false);
                                            }
                                        }}
                                    />

                                    {!isInputExpanded && (
                                        <div className="absolute bottom-2 right-2 flex items-center gap-1 bg-transparent">
                                            <button
                                                disabled={!isSignedIn || processing}
                                                onClick={toggleListening}
                                                className={`p-1.5 rounded-[2px] transition-colors disabled:opacity-30 ${isListening ? 'bg-cyan-500/20 text-cyan-400 animate-pulse' : 'bg-transparent hover:bg-cyan-500/10 text-zinc-500 hover:text-cyan-400'}`}
                                                title={!isSignedIn ? "Sign in to unlock Voice Copilot" : "Voice Command"}
                                            >
                                                <Mic className="w-4 h-4" />
                                            </button>
                                            <button disabled={processing} onClick={() => setIsInputExpanded(true)} className="p-1.5 rounded-[2px] bg-transparent hover:bg-emerald-500/20 text-zinc-500 hover:text-emerald-400 transition-colors disabled:opacity-30" title="Toggle Expansion">
                                                <Maximize2 className="w-3.5 h-3.5" />
                                            </button>
                                            <button disabled={processing} onClick={handleAsk} className="p-1.5 rounded-[2px] bg-white/5 hover:bg-emerald-500/20 text-emerald-500/50 hover:text-emerald-400 transition-colors disabled:opacity-30" title="Execute">
                                                <Terminal className="w-4 h-4" />
                                            </button>
                                        </div>
                                    )}
                                    {isInputExpanded && (
                                        <div className="flex justify-end mt-4 pt-4 border-t border-white/10 shrink-0">
                                            <button onClick={() => { setIsInputExpanded(false); handleAsk(); }} className="bg-emerald-600/20 hover:bg-emerald-500/30 text-emerald-500 border border-emerald-500/30 font-mono text-[11px] uppercase tracking-widest py-2 px-6 rounded transition-colors flex items-center gap-2">
                                                <Terminal className="w-3 h-3" /> Execute Command
                                            </button>
                                        </div>
                                    )}
                                </div>
                            </div>
                        </div>
                    )}
                </div>

                {/* Right Panel (Artifact Canvas) */}
                <div className="flex-1 bg-[#0a0a0a] relative flex flex-col z-0 overflow-hidden">
                    <CursorSpotlightGrid processing={processing} rightContent={rightContent} activeTab={activeTab} voiceActive={voiceActive} isListening={isListening}>

                        <div className="flex border-b border-white/5 bg-black/80 backdrop-blur-md relative z-20 px-6 pt-3 overflow-x-auto no-scrollbar">
                            {['data', 'logs', 'viz', 'strategy'].map((tab) => {
                                const isPremiumTab = tab === 'strategy';
                                const isLocked = isPremiumTab && !isSignedIn;
                                const TabButton = (
                                    <button
                                        key={tab}
                                        onClick={(e) => {
                                            if (!file) { e.preventDefault(); return; }
                                            if (!isLocked) setActiveTab(tab);
                                        }}
                                        className={`px-6 py-2.5 flex items-center gap-2 font-mono text-[10px] tracking-widest uppercase border-b-2 transition-all whitespace-nowrap ${activeTab === tab && file
                                            ? "border-emerald-400/80 text-emerald-400 bg-white/5"
                                            : "border-transparent text-zinc-600 hover:text-zinc-400 hover:bg-white/[0.02]"
                                            } ${!file && "opacity-30 cursor-not-allowed"} ${isLocked && "opacity-50 cursor-not-allowed"}`}
                                    >
                                        {tab === 'data' ? '[ Data View ]' : tab === 'logs' ? '[ Execution Pipeline ]' : tab === 'viz' ? '[ Visualizations ]' : '[ Strategy Brief ]'}
                                        {isLocked && <Lock className="w-3 h-3 ml-2 inline" />}
                                    </button>
                                );
                                return isLocked ? <SignInButton mode="modal" key={tab}>{TabButton}</SignInButton> : TabButton;
                            })}
                            <div className="ml-auto pb-2 flex items-end shrink-0 pl-6 hidden md:flex">
                                <span className="font-mono text-[9px] text-zinc-600 uppercase tracking-widest">Workspace: Isolated</span>
                            </div>
                        </div>

                        <div className={`flex-1 relative w-full h-full flex items-center justify-center overflow-y-auto ${rightContent === 'chart' && activeTab === 'viz' ? 'p-0' : 'p-8'}`}>
                            <AnimatePresence mode="wait">
                                {rightContent === 'table' && activeTab === 'data' && (
                                    <motion.div key="table" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.2 }} className="w-full flex justify-center">
                                        <DataTable headers={csvHeaders} data={csvPreviewData} isAudioActive={voiceActive || isListening} />
                                    </motion.div>
                                )}
                                {rightContent === 'processing' && activeTab === 'logs' && (
                                    <motion.div key="processing" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.2 }} className="w-full flex justify-center">
                                        <TerminalLoader currentTaskId={currentTaskId} onComplete={onProcessingComplete} />
                                    </motion.div>
                                )}
                                {rightContent === 'chart' && activeTab === 'viz' && (
                                    <motion.div key="chart" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.2 }} className="w-full h-full flex items-center justify-center">
                                        {/* The Upgraded Pipeline Canvas */}
                                        <VisualizerArtifact
                                            analysisTurns={analysisTurns}
                                            voiceActive={voiceActive}
                                            setVoiceActive={setVoiceActive}
                                            isSignedIn={isSignedIn}
                                        />
                                    </motion.div>
                                )}

                                {rightContent === 'chart' && activeTab === 'data' && (
                                    <motion.div key="table-retained" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.2 }} className="w-full flex justify-center">
                                        <DataTable headers={csvHeaders} data={csvPreviewData} isAudioActive={voiceActive || isListening} />
                                    </motion.div>
                                )}
                                {rightContent === 'chart' && activeTab === 'logs' && (
                                    <div key="logs-complete" className="font-mono text-xs text-zinc-400 flex flex-col p-8 border border-white/5 bg-[#0a0a0a]/80 backdrop-blur-md rounded-md z-10 w-full max-w-4xl h-full max-h-[80vh] overflow-hidden shadow-2xl">
                                        <div className="flex items-center justify-between border-b border-white/10 pb-4 mb-4 shrink-0">
                                            <div className="flex items-center gap-3">
                                                <Terminal className="w-4 h-4 text-emerald-500" />
                                                <span className="text-zinc-300 tracking-widest uppercase">Execution Pipeline</span>
                                            </div>
                                        </div>
                                        <div className="flex-1 overflow-y-auto custom-scrollbar pr-4 flex flex-col gap-6">
                                            <div className="mt-6 border-t border-zinc-800 pt-6">
                                                <div className="text-xs text-emerald-500 font-mono uppercase mb-2 flex items-center gap-2">
                                                    <span className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></span>
                                                    Pandas Preprocessing Telemetry
                                                </div>
                                                <div className="text-xs text-zinc-400 font-mono border-l-2 border-emerald-500/50 pl-3 mb-6 bg-emerald-500/5 p-3 rounded-r">
                                                    {aiAnalysisText?.preprocessing_log || "Awaiting dataset ingestion..."}
                                                </div>
                                                <div className="text-xs text-blue-400 font-mono uppercase mb-2 flex items-center gap-2">
                                                    <span className="w-2 h-2 rounded-full bg-blue-400 animate-pulse"></span>
                                                    Dynamic SQL Kernel Execution
                                                </div>
                                                <pre className="bg-zinc-900 p-4 rounded border border-zinc-800 text-xs text-zinc-300 font-mono overflow-x-auto shadow-inner">
                                                    <code>{aiAnalysisText?.raw_sql || "Awaiting AI query generation..."}</code>
                                                </pre>
                                            </div>
                                        </div>
                                    </div>
                                )}
                                {/* Strategy Brief */}
                                {rightContent === 'chart' && activeTab === 'strategy' && (
                                    <motion.div key="strategy" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.2 }} className="w-full h-full flex justify-center py-8 overflow-y-auto custom-scrollbar relative">
                                        <StrategyRenderer
                                            data={aiAnalysisText?.strategy_brief}
                                            audioUrl={aiAnalysisText?.audio_url}
                                            voiceActive={voiceActive}
                                            setVoiceActive={setVoiceActive}
                                            setIsChatOpen={setIsChatOpen}
                                            playSFX={playSFX}
                                        />
                                    </motion.div>
                                )}
                            </AnimatePresence>
                        </div>
                    </CursorSpotlightGrid>
                </div>
            </div>

            {/* Mobile Toggle */}
            {rightContent !== 'empty' && (
                <button onClick={() => setMobilePanelOpen(true)} className={`md:hidden fixed bottom-10 right-6 z-[60] w-12 h-12 bg-emerald-500 rounded-full flex items-center justify-center text-black shadow-[0_0_20px_rgba(16,185,129,0.4)] transition-all duration-300 ${mobilePanelOpen ? 'scale-0' : 'scale-100'}`}>
                    <Terminal className="w-5 h-5" />
                </button>
            )}

            {/* Telemetry Footer */}
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

            {/* Paywall Modal */}
            <AnimatePresence>
                {showPaywall && (
                    <motion.div initial={{ opacity: 0, backdropFilter: "blur(0px)" }} animate={{ opacity: 1, backdropFilter: "blur(12px)" }} exit={{ opacity: 0, backdropFilter: "blur(0px)" }} transition={{ duration: 0.2 }} className="fixed inset-0 z-[100] flex items-center justify-center bg-black/80 p-4">
                        <motion.div initial={{ opacity: 0, scale: 0.98, y: 5 }} animate={{ opacity: 1, scale: 1, y: 0 }} transition={{ duration: 0.2, ease: "easeOut" }} className="w-full max-w-md bg-[#0a0a0a] text-zinc-100 border border-zinc-800 shadow-[0_0_80px_rgba(0,0,0,0.9)] relative overflow-hidden rounded-[2px]">
                            <div className="w-full h-1 bg-white" />
                            <div className="p-8">
                                <Lock className="w-5 h-5 text-zinc-400 mb-6" strokeWidth={1.5} />
                                {!isSignedIn ? (
                                    <>
                                        <h2 className="text-lg font-medium tracking-tight mb-2 uppercase text-white font-sans">Free Limit Reached</h2>
                                        <div className="border border-white/5 bg-zinc-900/30 p-5 rounded-[2px] mb-8 relative overflow-hidden group">
                                            <p className="text-[13px] text-zinc-300 font-sans leading-relaxed relative z-10">Sign in to unlock 15 daily queries, the Executive Strategy Brief, and Amazon Nova Sonic Voice Agents.</p>
                                        </div>
                                        <div className="flex flex-col gap-3">
                                            <SignInButton mode="modal">
                                                <button onClick={() => setShowPaywall(false)} className="w-full bg-emerald-500 text-black font-semibold py-3 flex items-center justify-center gap-2 hover:bg-emerald-400 transition-colors uppercase text-xs tracking-widest rounded-[2px]">Sign in to Unlock</button>
                                            </SignInButton>
                                            <button onClick={() => setShowPaywall(false)} className="w-full text-zinc-600 font-mono text-[10px] py-2 hover:text-zinc-400 uppercase tracking-widest mt-1">[ Abort Session ]</button>
                                        </div>
                                    </>
                                ) : (
                                    <>
                                        <h2 className="text-lg font-medium tracking-tight mb-2 uppercase text-white font-sans">Maximum Compute Reached</h2>
                                        <p className="text-zinc-500 text-[11px] mb-8 font-mono leading-relaxed uppercase tracking-wider">&gt; ERR_QUOTA_EXHAUSTED<br />&gt; 402_PAYMENT_REQUIRED</p>
                                        <div className="border border-white/5 bg-zinc-900/30 p-5 rounded-[2px] mb-8 relative overflow-hidden group">
                                            <div className="absolute inset-0 bg-emerald-500/5 opacity-0 group-hover:opacity-100 transition-opacity"></div>
                                            <p className="text-[13px] text-zinc-300 mb-4 font-sans leading-relaxed relative z-10">Upgrade to Premium for Unlimited Enterprise Access.</p>
                                            <ul className="space-y-3 text-[10px] font-mono text-zinc-400 uppercase tracking-wider relative z-10">
                                                <li className="flex items-center gap-3"><Activity className="w-3 h-3 text-emerald-500/80" /> Unlimited Analysis Streams</li>
                                                <li className="flex items-center gap-3"><Database className="w-3 h-3 text-emerald-500/80" /> Datasets up to 500MB</li>
                                                <li className="flex items-center gap-3"><Terminal className="w-3 h-3 text-emerald-500/80" /> Dedicated Compute Nodes</li>
                                            </ul>
                                        </div>
                                        <div className="flex flex-col gap-3">
                                            <button onClick={() => setShowPaywall(false)} className="w-full bg-white text-black font-semibold py-3 flex items-center justify-center gap-2 hover:bg-zinc-200 transition-colors uppercase text-xs tracking-widest rounded-[2px]">Authorize Payment</button>
                                            <button onClick={() => setShowPaywall(false)} className="w-full text-zinc-600 font-mono text-[10px] py-2 hover:text-zinc-400 uppercase tracking-widest mt-1">[ Abort Session ]</button>
                                        </div>
                                    </>
                                )}
                            </div>
                        </motion.div>
                    </motion.div>
                )}
            </AnimatePresence>
        </div>
    );
}
