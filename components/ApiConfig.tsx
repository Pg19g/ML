
import React, { useState } from 'react';
import Card from './ui/Card';

const EyeIcon: React.FC<React.SVGProps<SVGSVGElement>> = (props) => (
    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" {...props}>
      <path strokeLinecap="round" strokeLinejoin="round" d="M2.036 12.322a1.012 1.012 0 010-.639C3.423 7.51 7.36 4.5 12 4.5c4.638 0 8.573 3.007 9.963 7.178.07.207.07.431 0 .639C20.577 16.49 16.64 19.5 12 19.5c-4.638 0-8.573-3.007-9.963-7.178z" />
      <path strokeLinecap="round" strokeLinejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
    </svg>
);

const EyeSlashIcon: React.FC<React.SVGProps<SVGSVGElement>> = (props) => (
    <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" {...props}>
      <path strokeLinecap="round" strokeLinejoin="round" d="M3.98 8.223A10.477 10.477 0 001.934 12C3.226 16.338 7.244 19.5 12 19.5c.993 0 1.953-.138 2.863-.395M6.228 6.228A10.45 10.45 0 0112 4.5c4.756 0 8.773 3.162 10.065 7.498a10.523 10.523 0 01-4.293 5.774M6.228 6.228L3 3m3.228 3.228l3.65 3.65m7.894 7.894L21 21m-3.228-3.228l-3.65-3.65m0 0a3 3 0 10-4.243-4.243m4.243 4.243L6.228 6.228" />
    </svg>
);

interface ApiConfigProps {
  isKeySet: boolean;
  onSetKey: (key: string) => void;
}

const ApiConfig: React.FC<ApiConfigProps> = ({ isKeySet, onSetKey }) => {
  const [keyInput, setKeyInput] = useState('');
  const [showKey, setShowKey] = useState(false);

  const handleSave = () => {
    if (keyInput.trim()) {
      onSetKey(keyInput.trim());
      setKeyInput(''); 
    }
  };

  return (
    <Card title="API Configuration" subtitle="EODHD API Key">
      <div className="space-y-3">
        <p className="text-sm text-gray-400">
          An EODHD API key is required to ingest market data. It's stored only for this session.
        </p>
        <div className="flex items-center space-x-2">
            <div className="relative flex-grow">
                 <input
                    type={showKey ? 'text' : 'password'}
                    value={keyInput}
                    onChange={(e) => setKeyInput(e.target.value)}
                    placeholder={isKeySet ? "Key is set. Enter to update." : "Enter your API key..."}
                    className="w-full bg-gray-900 rounded-md py-2 px-3 text-gray-200 placeholder-gray-500 focus:outline-none focus:ring-2 focus:ring-cyan-500 border border-gray-600"
                />
                 <button 
                    type="button" 
                    onClick={() => setShowKey(!showKey)}
                    className="absolute inset-y-0 right-0 pr-3 flex items-center text-gray-400 hover:text-gray-200"
                    aria-label={showKey ? "Hide key" : "Show key"}
                >
                    {showKey ? <EyeSlashIcon className="h-5 w-5"/> : <EyeIcon className="h-5 w-5"/>}
                 </button>
            </div>
          <button
            onClick={handleSave}
            className="px-4 py-2 bg-cyan-600 hover:bg-cyan-700 rounded-md text-white font-semibold text-sm transition-colors focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-offset-gray-800 focus:ring-cyan-500 disabled:opacity-50 disabled:cursor-not-allowed"
            disabled={!keyInput.trim()}
          >
            Save
          </button>
        </div>
        <div className="text-xs text-gray-500 flex items-center pt-1">
          <div className={`w-2.5 h-2.5 rounded-full mr-2 ${isKeySet ? 'bg-green-500 animate-pulse' : 'bg-red-500'}`}></div>
          {isKeySet ? 'API Key is set and ready.' : 'API Key is not set.'}
        </div>
      </div>
    </Card>
  );
};

export default ApiConfig;
