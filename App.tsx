
import React from 'react';
import DashboardPage from './pages/DashboardPage';
import Header from './components/Header';
import GeminiChat from './components/GeminiChat';

const App: React.FC = () => {
  return (
    <div className="min-h-screen bg-gray-900 text-gray-200 font-sans">
      <Header />
      <main className="p-4 sm:p-6 lg:p-8">
        <DashboardPage />
      </main>
      <GeminiChat />
    </div>
  );
};

export default App;
