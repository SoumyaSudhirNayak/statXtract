import { useState } from "react";
import { useNavigate } from "react-router";
import { Header } from "../../components/Header";
import { Navigation } from "../../components/Navigation";
import { BackButton } from "../../components/BackButton";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function ChangePassword() {
  const navigate = useNavigate();
  const adminData = JSON.parse(sessionStorage.getItem("adminData") || '{"fullName":"Admin"}');
  const [formData, setFormData] = useState({
    currentPassword: "",
    newPassword: "",
    confirmPassword: "",
  });
  const [hoveredInput, setHoveredInput] = useState<string | null>(null);
  const [isMainHovered, setIsMainHovered] = useState(false);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    alert("Password updated successfully");
    navigate("/admin/dashboard");
  };

  return (
    <div className="min-h-screen relative">
      <AnimatedBackground />
      
      <div className="relative z-10">
        <Header userName={adminData.fullName} userRole="admin" />
        <Navigation userRole="admin" />

        <div className="max-w-[1200px] mx-auto px-10 py-12">
          <div className="mb-10 flex justify-between items-center">
            <BackButton to="/admin/dashboard" />
            <h1 className="text-4xl text-[#1C1C1C] m-0" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>Change Password</h1>
            <div className="w-[80px]"></div>
          </div>

          <form 
            onSubmit={handleSubmit} 
            onMouseEnter={() => setIsMainHovered(true)}
            onMouseLeave={() => setIsMainHovered(false)}
            className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-2xl p-12 transition-all duration-300 shadow-xl max-w-[800px] mx-auto"
            style={{
              borderColor: isMainHovered ? '#2E5BBA' : '#D1D5DB',
              boxShadow: isMainHovered ? '0 15px 40px rgba(46, 91, 186, 0.2)' : 'none'
            }}
          >
            <div className="space-y-10">
              <div>
                <label className="block text-[#1C1C1C] mb-4 font-bold text-xl">Current Password</label>
                <input
                  type="password"
                  value={formData.currentPassword}
                  onChange={(e) => setFormData({ ...formData, currentPassword: e.target.value })}
                  onMouseEnter={() => setHoveredInput('current-password')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                  style={{
                    borderColor: hoveredInput === 'current-password' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredInput === 'current-password' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-4 font-bold text-xl">New Password</label>
                <input
                  type="password"
                  value={formData.newPassword}
                  onChange={(e) => setFormData({ ...formData, newPassword: e.target.value })}
                  onMouseEnter={() => setHoveredInput('new-password')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                  style={{
                    borderColor: hoveredInput === 'new-password' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredInput === 'new-password' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-4 font-bold text-xl">Confirm New Password</label>
                <input
                  type="password"
                  value={formData.confirmPassword}
                  onChange={(e) => setFormData({ ...formData, confirmPassword: e.target.value })}
                  onMouseEnter={() => setHoveredInput('confirm-password')}
                  onMouseLeave={() => setHoveredInput(null)}
                  required
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl font-bold"
                  style={{
                    borderColor: hoveredInput === 'confirm-password' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredInput === 'confirm-password' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                />
              </div>

              <div 
                onMouseEnter={() => setHoveredInput('requirements')}
                onMouseLeave={() => setHoveredInput(null)}
                className="bg-[#F5F7FA] border-2 rounded-xl p-8 transition-all duration-300"
                style={{
                  borderColor: hoveredInput === 'requirements' ? '#2E7D32' : '#D1D5DB',
                  boxShadow: hoveredInput === 'requirements' ? '0 0 15px rgba(46, 125, 50, 0.2)' : 'none'
                }}
              >
                <p className="text-[#1C1C1C] text-lg m-0 mb-4 font-black uppercase tracking-widest">Password Requirements:</p>
                <ul className="text-[#6B7280] text-xl space-y-3 m-0 pl-8 list-disc">
                  <li>At least 8 characters long</li>
                  <li>Include uppercase and lowercase letters</li>
                  <li>Include at least one number</li>
                  <li>Include at least one special character</li>
                </ul>
              </div>

              <div className="flex gap-6 mt-6">
                <button
                  type="submit"
                  onMouseEnter={() => setHoveredInput('update-btn')}
                  onMouseLeave={() => setHoveredInput(null)}
                  className="lift-on-hover flex-1 bg-[#2E5BBA] text-white py-5 rounded-xl border-2 cursor-pointer transition-all duration-300 text-2xl font-black shadow-lg"
                  style={{
                    borderColor: hoveredInput === 'update-btn' ? '#F4A300' : '#2E5BBA',
                    boxShadow: hoveredInput === 'update-btn' ? '0 10px 25px rgba(244, 163, 0, 0.4)' : 'none'
                  }}
                >
                  Update Password
                </button>
                <button
                  type="button"
                  onClick={() => navigate("/admin/dashboard")}
                  onMouseEnter={() => setHoveredInput('cancel-btn')}
                  onMouseLeave={() => setHoveredInput(null)}
                  className="lift-on-hover flex-1 bg-white text-[#1C1C1C] py-5 rounded-xl border-2 cursor-pointer transition-all duration-300 text-2xl font-black"
                  style={{
                    borderColor: hoveredInput === 'cancel-btn' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredInput === 'cancel-btn' ? '0 0 15px rgba(46, 91, 186, 0.1)' : 'none'
                  }}
                >
                  Cancel
                </button>
              </div>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}