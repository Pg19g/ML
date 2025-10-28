
import React from 'react';

interface ToggleProps {
  label: string;
  enabled: boolean;
  onChange: (enabled: boolean) => void;
  Icon?: React.ElementType;
}

const Toggle: React.FC<ToggleProps> = ({ label, enabled, onChange, Icon }) => {
  return (
    <div className="flex items-center space-x-2 cursor-pointer" onClick={() => onChange(!enabled)}>
      {Icon && <Icon className={`w-5 h-5 transition-colors ${enabled ? 'text-purple-400' : 'text-gray-400'}`} />}
      <span className={`text-sm font-medium transition-colors ${enabled ? 'text-gray-100' : 'text-gray-400'}`}>{label}</span>
      <div
        className={`${
          enabled ? 'bg-purple-600' : 'bg-gray-600'
        } relative inline-flex h-5 w-9 items-center rounded-full transition-colors`}
      >
        <span
          className={`${
            enabled ? 'translate-x-5' : 'translate-x-1'
          } inline-block h-3 w-3 transform rounded-full bg-white transition-transform`}
        />
      </div>
    </div>
  );
};

export default Toggle;
