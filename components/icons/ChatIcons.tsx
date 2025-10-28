
import React from 'react';

export const BotIcon: React.FC = () => (
  <div className="w-8 h-8 rounded-full bg-gray-600 flex-shrink-0 flex items-center justify-center">
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-5 h-5 text-cyan-400">
      <path d="M10 2a.75.75 0 01.75.75v1.5a.75.75 0 01-1.5 0v-1.5A.75.75 0 0110 2zM10 15a.75.75 0 01.75.75v1.5a.75.75 0 01-1.5 0v-1.5A.75.75 0 0110 15zM10 7a3 3 0 100 6 3 3 0 000-6z" />
      <path fillRule="evenodd" d="M1.323 10.933a8.002 8.002 0 0110.365-7.465.75.75 0 01.815 1.255A6.5 6.5 0 005.158 13.52.75.75 0 014.004 14.5a8.002 8.002 0 01-2.68-3.567zM18.677 9.067a8.002 8.002 0 01-10.365 7.465.75.75 0 11-.815-1.255A6.5 6.5 0 0014.842 6.48.75.75 0 0115.996 5.5a8.002 8.002 0 012.68 3.567z" clipRule="evenodd" />
    </svg>
  </div>
);

export const UserIcon: React.FC = () => (
  <div className="w-8 h-8 rounded-full bg-gray-600 flex-shrink-0 flex items-center justify-center">
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-5 h-5 text-gray-300">
      <path d="M10 8a3 3 0 100-6 3 3 0 000 6zM3.465 14.493a1.23 1.23 0 00.41 1.412A9.957 9.957 0 0010 18c2.31 0 4.438-.784 6.131-2.095a1.23 1.23 0 00.41-1.412A9.995 9.995 0 0010 12c-2.31 0-4.438.784-6.131 2.095z" />
    </svg>
  </div>
);

export const SendIcon: React.FC = () => (
  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-5 h-5">
    <path d="M3.105 2.289a.75.75 0 00-.826.95l1.414 4.949a.75.75 0 00.95.535l4.64-1.656a.25.25 0 01.322.322l-1.656 4.64a.75.75 0 00.535.95l4.95 1.414a.75.75 0 00.95-.826l-2.289-8.01a.75.75 0 00-.733-.526l-8.01-2.29z" />
  </svg>
);

export const ThinkingIcon: React.FC<React.SVGProps<SVGSVGElement>> = (props) => (
  <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" strokeWidth={1.5} stroke="currentColor" {...props}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M8.25 3v1.5M4.5 8.25H3m18 0h-1.5M4.5 12H3m18 0h-1.5m-15 3.75H3m18 0h-1.5M8.25 21v-1.5M12 3v1.5m0 15V21m3.75-18v1.5M12 8.25a.75.75 0 01.75.75v3.75a.75.75 0 01-1.5 0V9a.75.75 0 01.75-.75zm0 8.25a.75.75 0 100-1.5.75.75 0 000 1.5z" />
    <path strokeLinecap="round" strokeLinejoin="round" d="M12 1.5c-5.936 0-10.75 4.814-10.75 10.75 0 5.936 4.814 10.75 10.75 10.75s10.75-4.814 10.75-10.75C22.75 6.314 17.936 1.5 12 1.5z" />
  </svg>
);

export const LightbulbIcon: React.FC = () => (
    <svg xmlns="http://www.w3.org/2000/svg" className="h-10 w-10 mx-auto" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1}>
      <path strokeLinecap="round" strokeLinejoin="round" d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
    </svg>
);

export const CloseIcon: React.FC = () => (
    <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
    </svg>
);

export const SparklesIcon: React.FC = () => (
  <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 3v4M3 5h4M6 17v4m-2-2h4m5-16l2.293 2.293a1 1 0 010 1.414L10 16l-4 4-4-4 5.293-5.293a1 1 0 011.414 0L13 13.586m0 0l4.243 4.243a2 2 0 010 2.828l-1.414 1.414a2 2 0 01-2.828 0L10 16.586m0 0l-1.293-1.293a1 1 0 00-1.414 0L3 19.586m12-12l-1.414-1.414a2 2 0 00-2.828 0L10 8.586m0 0L8.707 7.293a1 1 0 00-1.414 0L3 11.586" />
  </svg>
)
