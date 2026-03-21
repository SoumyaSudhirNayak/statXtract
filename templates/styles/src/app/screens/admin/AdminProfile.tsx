import { useState } from "react";
import { useNavigate } from "react-router";
import { Header } from "../../components/Header";
import { Navigation } from "../../components/Navigation";
import { BackButton } from "../../components/BackButton";
import { User, Mail, Shield, LogOut } from "lucide-react";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function AdminProfile() {
  const navigate = useNavigate();
  const [isMainHovered, setIsMainHovered] = useState(false);
  const [hoveredField, setHoveredField] = useState<string | null>(null);
  
  const adminData = JSON.parse(
    sessionStorage.getItem("adminData") || '{"fullName":"Admin","email":"admin@example.com","role":"Administrator"}'
  );

  const handleLogout = () => {
    sessionStorage.clear();
    navigate("/module-selection");
  };

  return (
    <div className="min-h-screen relative">
      <AnimatedBackground />
      
      <div className="relative z-10">
        <Header userName={adminData.fullName} userRole="admin" />
        <Navigation userRole="admin" />

        <div className="max-w-[1600px] mx-auto px-10 py-12">
          <div className="mb-10 flex justify-between items-center">
            <BackButton to="/admin/dashboard" />
            <h1 className="text-4xl text-[#1C1C1C] m-0" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>My Profile</h1>
            <div className="w-[80px]"></div>
          </div>

          <div className="max-w-[1000px] mx-auto">
            {/* Profile Card */}
            <div 
              onMouseEnter={() => setIsMainHovered(true)}
              onMouseLeave={() => setIsMainHovered(false)}
              className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-2xl shadow-xl p-12 mb-10 transition-all duration-300"
              style={{
                borderColor: isMainHovered ? '#2E5BBA' : '#D1D5DB',
                boxShadow: isMainHovered ? '0 20px 50px rgba(46, 91, 186, 0.2)' : 'none'
              }}
            >
              <div className="flex items-center gap-8 mb-10 pb-10 border-b-4 border-[#2E5BBA]/30">
                <div className="w-32 h-32 bg-[#2E5BBA] rounded-3xl flex items-center justify-center shadow-lg">
                  <User className="w-16 h-16 text-white" />
                </div>
                <div>
                  <h3 className="text-4xl text-[#1C1C1C] m-0 mb-3 font-black" style={{ fontFamily: 'var(--font-head)' }}>{adminData.fullName}</h3>
                  <p className="text-2xl text-[#6B7280] m-0 flex items-center gap-3 font-bold">
                    <Shield className="w-6 h-6" />
                    Administrator
                  </p>
                </div>
              </div>

              {/* Profile Details */}
              <div className="space-y-10">
                <div>
                  <label className="block text-[#6B7280] text-sm mb-3 font-black uppercase tracking-widest">
                    Full Name
                  </label>
                  <div 
                    onMouseEnter={() => setHoveredField('name')}
                    onMouseLeave={() => setHoveredField(null)}
                    className="flex items-center gap-5 bg-[#F5F7FA] border-2 rounded-xl px-6 py-5 transition-all duration-300"
                    style={{
                      borderColor: hoveredField === 'name' ? '#2E5BBA' : '#E5E7EB',
                      boxShadow: hoveredField === 'name' ? '0 10px 20px rgba(46, 91, 186, 0.1)' : 'none'
                    }}
                  >
                    <User className="w-8 h-8 text-[#2E5BBA]" />
                    <p className="text-2xl text-[#1C1C1C] m-0 font-bold">{adminData.fullName}</p>
                  </div>
                </div>

                <div>
                  <label className="block text-[#6B7280] text-sm mb-3 font-black uppercase tracking-widest">
                    Email Address
                  </label>
                  <div 
                    onMouseEnter={() => setHoveredField('email')}
                    onMouseLeave={() => setHoveredField(null)}
                    className="flex items-center gap-5 bg-[#F5F7FA] border-2 rounded-xl px-6 py-5 transition-all duration-300"
                    style={{
                      borderColor: hoveredField === 'email' ? '#2E5BBA' : '#E5E7EB',
                      boxShadow: hoveredField === 'email' ? '0 10px 20px rgba(46, 91, 186, 0.1)' : 'none'
                    }}
                  >
                    <Mail className="w-8 h-8 text-[#2E5BBA]" />
                    <p className="text-2xl text-[#1C1C1C] m-0 font-bold">{adminData.email}</p>
                  </div>
                </div>

                <div>
                  <label className="block text-[#6B7280] text-sm mb-3 font-black uppercase tracking-widest">
                    Role
                  </label>
                  <div 
                    onMouseEnter={() => setHoveredField('role')}
                    onMouseLeave={() => setHoveredField(null)}
                    className="flex items-center gap-5 bg-[#F5F7FA] border-2 rounded-xl px-6 py-5 transition-all duration-300"
                    style={{
                      borderColor: hoveredField === 'role' ? '#2E5BBA' : '#E5E7EB',
                      boxShadow: hoveredField === 'role' ? '0 10px 20px rgba(46, 91, 186, 0.1)' : 'none'
                    }}
                  >
                    <Shield className="w-8 h-8 text-[#2E5BBA]" />
                    <p className="text-2xl text-[#1C1C1C] m-0 font-bold">Administrator</p>
                  </div>
                </div>
              </div>
            </div>

            {/* Logout Button */}
            <button
              onClick={handleLogout}
              className="lift-on-hover w-full flex items-center justify-center gap-4 bg-[#d4183d] text-white py-6 rounded-2xl border-2 border-[#d4183d] hover:border-[#F4A300] cursor-pointer transition-all duration-300 hover:bg-[#b01530] shadow-xl text-2xl font-black"
            >
              <LogOut className="w-8 h-8" />
              <span>Logout</span>
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
