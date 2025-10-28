
import React, { useState, useRef, useEffect } from 'react';
import { analyzeWithGemini } from '../services/geminiService';
import { ChatMessage } from '../types';
import { BotIcon, SendIcon, UserIcon, ThinkingIcon, LightbulbIcon, CloseIcon, SparklesIcon } from './icons/ChatIcons';
import Toggle from './ui/Toggle';

const GeminiChat: React.FC = () => {
  const [isOpen, setIsOpen] = useState(false);
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [isThinkingMode, setIsThinkingMode] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(scrollToBottom, [messages]);

  const handleSend = async () => {
    if (input.trim() === '' || isLoading) return;

    const userMessage: ChatMessage = { id: Date.now().toString(), role: 'user', text: input };
    setMessages(prev => [...prev, userMessage]);
    setInput('');
    setIsLoading(true);

    const botResponseText = await analyzeWithGemini(input, isThinkingMode);
    
    const botMessage: ChatMessage = { id: (Date.now() + 1).toString(), role: 'model', text: botResponseText };
    setMessages(prev => [...prev, botMessage]);
    setIsLoading(false);
  };
  
  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      handleSend();
    }
  };

  if (!isOpen) {
    return (
      <button
        onClick={() => setIsOpen(true)}
        className="fixed bottom-6 right-6 bg-cyan-600 hover:bg-cyan-500 text-white rounded-full p-4 shadow-lg transition-transform hover:scale-110 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-gray-900 focus:ring-cyan-500 z-50"
        aria-label="Open AI Assistant"
      >
        <SparklesIcon />
      </button>
    );
  }

  return (
    <div className="fixed bottom-6 right-6 w-[90vw] max-w-md h-[70vh] max-h-[600px] bg-gray-800 rounded-2xl shadow-2xl flex flex-col z-50 border border-gray-700">
      {/* Header */}
      <div className="flex items-center justify-between p-4 border-b border-gray-700 flex-shrink-0">
        <div className="flex items-center space-x-2">
          <h2 className="text-lg font-bold text-gray-100">AI Assistant</h2>
          <div className={`w-2.5 h-2.5 rounded-full ${isThinkingMode ? 'bg-purple-400' : 'bg-green-400'}`}></div>
        </div>
        <div className="flex items-center space-x-4">
          <Toggle
            label="Thinking Mode"
            enabled={isThinkingMode}
            onChange={setIsThinkingMode}
            Icon={ThinkingIcon}
          />
          <button onClick={() => setIsOpen(false)} className="text-gray-400 hover:text-white"><CloseIcon /></button>
        </div>
      </div>

      {/* Messages */}
      <div className="flex-grow p-4 overflow-y-auto space-y-6">
        {messages.length === 0 && (
           <div className="text-center text-gray-400 mt-8">
             <LightbulbIcon />
             <p className="mt-2 text-sm">Ask me to analyze the report, explain concepts, or suggest improvements.</p>
           </div>
        )}
        {messages.map((msg) => (
          <div key={msg.id} className={`flex items-start gap-3 ${msg.role === 'user' ? 'justify-end' : ''}`}>
            {msg.role === 'model' && <BotIcon />}
            <div className={`max-w-xs md:max-w-sm px-4 py-2 rounded-2xl ${msg.role === 'user' ? 'bg-cyan-600 text-white rounded-br-lg' : 'bg-gray-700 text-gray-200 rounded-bl-lg'}`}>
              <p className="text-sm whitespace-pre-wrap">{msg.text}</p>
            </div>
            {msg.role === 'user' && <UserIcon />}
          </div>
        ))}
        {isLoading && (
          <div className="flex items-start gap-3">
            <BotIcon />
            <div className="px-4 py-3 rounded-2xl bg-gray-700 rounded-bl-lg">
              <div className="flex items-center space-x-2">
                <div className="w-2 h-2 bg-gray-400 rounded-full animate-pulse [animation-delay:-0.3s]"></div>
                <div className="w-2 h-2 bg-gray-400 rounded-full animate-pulse [animation-delay:-0.15s]"></div>
                <div className="w-2 h-2 bg-gray-400 rounded-full animate-pulse"></div>
              </div>
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <div className="p-4 border-t border-gray-700 flex-shrink-0">
        <div className="relative">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder={isThinkingMode ? "Ask a complex question..." : "Ask a quick question..."}
            className="w-full bg-gray-700 rounded-full py-2 pl-4 pr-12 text-gray-200 placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-cyan-500"
          />
          <button onClick={handleSend} className="absolute right-2 top-1/2 -translate-y-1/2 p-2 bg-cyan-600 hover:bg-cyan-700 rounded-full text-white disabled:bg-gray-600" disabled={isLoading}>
            <SendIcon />
          </button>
        </div>
      </div>
    </div>
  );
};

export default GeminiChat;
