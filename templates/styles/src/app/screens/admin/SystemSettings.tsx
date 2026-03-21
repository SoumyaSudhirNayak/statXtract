import { useState } from "react";
import { useNavigate } from "react-router";
import { Header } from "../../components/Header";
import { Navigation } from "../../components/Navigation";
import { BackButton } from "../../components/BackButton";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function SystemSettings() {
  const navigate = useNavigate();
  const adminData = JSON.parse(sessionStorage.getItem("adminData") || '{"fullName":"Admin"}');
  const [settings, setSettings] = useState({
    maintenanceMode: false,
    autoBackup: true,
    dataRetention: "90",
    maxUploadSize: "100",
  });
  const [isMainHovered, setIsMainHovered] = useState(false);

  return (
    <div className="min-h-screen relative">
      <AnimatedBackground />
      
      <div className="relative z-10">
        <Header userName={adminData.fullName} userRole="admin" />
        <Navigation userRole="admin" />

        <div className="max-w-[1400px] mx-auto px-10 py-12">
          <div className="mb-10 flex justify-between items-center">
            <BackButton to="/admin/dashboard" />
            <h1 className="text-4xl text-[#1C1C1C] m-0" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>System Settings</h1>
            <div className="w-[80px]"></div>
          </div>

          <div 
            onMouseEnter={() => setIsMainHovered(true)}
            onMouseLeave={() => setIsMainHovered(false)}
            className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-xl p-12 transition-all duration-300 shadow-xl"
            style={{
              borderColor: isMainHovered ? '#2E5BBA' : '#D1D5DB',
              boxShadow: isMainHovered ? '0 15px 40px rgba(46, 91, 186, 0.2)' : 'none'
            }}
          >
            <div className="space-y-10">
              <div className="flex items-center justify-between py-6 border-b-4 border-[#2E5BBA]/20">
                <div>
                  <h4 className="text-2xl text-[#1C1C1C] m-0 mb-2 font-black">Maintenance Mode</h4>
                  <p className="text-[#6B7280] text-lg m-0 font-medium">Temporarily disable user access</p>
                </div>
                <label className="relative inline-block w-20 h-10">
                  <input
                    type="checkbox"
                    checked={settings.maintenanceMode}
                    onChange={(e) => setSettings({ ...settings, maintenanceMode: e.target.checked })}
                    className="sr-only peer"
                  />
                  <span className="absolute cursor-pointer inset-0 bg-[#E5E7EB] rounded-full transition-all peer-checked:bg-[#2E7D32] before:absolute before:content-[''] before:h-8 before:w-8 before:left-1 before:top-1 before:bg-white before:rounded-full before:transition-all peer-checked:before:translate-x-10 shadow-inner"></span>
                </label>
              </div>

              <div className="flex items-center justify-between py-6 border-b-4 border-[#2E5BBA]/20">
                <div>
                  <h4 className="text-2xl text-[#1C1C1C] m-0 mb-2 font-black">Automatic Backup</h4>
                  <p className="text-[#6B7280] text-lg m-0 font-medium">Enable daily automated backups</p>
                </div>
                <label className="relative inline-block w-20 h-10">
                  <input
                    type="checkbox"
                    checked={settings.autoBackup}
                    onChange={(e) => setSettings({ ...settings, autoBackup: e.target.checked })}
                    className="sr-only peer"
                  />
                  <span className="absolute cursor-pointer inset-0 bg-[#E5E7EB] rounded-full transition-all peer-checked:bg-[#2E7D32] before:absolute before:content-[''] before:h-8 before:w-8 before:left-1 before:top-1 before:bg-white before:rounded-full before:transition-all peer-checked:before:translate-x-10 shadow-inner"></span>
                </label>
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-4 font-bold text-xl">Data Retention Period (days)</label>
                <input
                  type="number"
                  value={settings.dataRetention}
                  onChange={(e) => setSettings({ ...settings, dataRetention: e.target.value })}
                  onMouseEnter={() => setIsMainHovered(true)}
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 hover:border-[#2E5BBA] hover:shadow-[0_0_20px_rgba(46,91,186,0.2)] text-xl font-bold"
                  style={{
                    borderColor: '#D1D5DB',
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-4 font-bold text-xl">Maximum Upload Size (MB)</label>
                <input
                  type="number"
                  value={settings.maxUploadSize}
                  onChange={(e) => setSettings({ ...settings, maxUploadSize: e.target.value })}
                  onMouseEnter={() => setIsMainHovered(true)}
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 hover:border-[#2E5BBA] hover:shadow-[0_0_20px_rgba(46,91,186,0.2)] text-xl font-bold"
                  style={{
                    borderColor: '#D1D5DB',
                  }}
                />
              </div>

              <button 
                className="lift-on-hover w-full bg-[#2E5BBA] text-white py-5 rounded-xl transition-all duration-300 border-2 cursor-pointer text-2xl font-black shadow-lg mt-6"
                style={{
                  borderColor: '#2E5BBA',
                }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.borderColor = '#F4A300';
                  e.currentTarget.style.boxShadow = '0 10px 25px rgba(244, 163, 0, 0.4)';
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.borderColor = '#2E5BBA';
                  e.currentTarget.style.boxShadow = '0 4px 15px rgba(0,0,0,0.1)';
                }}
              >
                Save Settings
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}