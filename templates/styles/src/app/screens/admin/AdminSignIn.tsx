import { useState, useRef } from "react";
import { useNavigate } from "react-router";
import { BackButton } from "../../components/BackButton";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function AdminSignIn() {
  const navigate = useNavigate();
  const [formData, setFormData] = useState({
    email: "",
    password: "",
  });
  const [hoveredInput, setHoveredInput] = useState<string | null>(null);
  const [isContainerHovered, setIsContainerHovered] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    // Mock authentication - store admin data
    const adminData = {
      fullName: "Admin User",
      email: formData.email,
    };
    sessionStorage.setItem("adminData", JSON.stringify(adminData));
    navigate("/admin/dashboard");
  };

  return (
    <div className="min-h-screen relative">
      <AnimatedBackground />
      
      <div className="relative z-10">
        <div className="bg-[#1F3A5F] text-white px-10 py-6 shadow-xl relative z-50">
          <div className="max-w-[1800px] mx-auto flex justify-between items-center">
            <h1 
              className="text-4xl m-0 font-black tracking-tighter cursor-pointer hover:opacity-80 transition-opacity" 
              style={{ fontFamily: 'var(--font-head)', color: 'white' }}
              onClick={() => navigate("/")}
            >
              STATXTRACT
            </h1>
          </div>
        </div>

        <div className="max-w-[800px] mx-auto px-6 py-20">
          <div className="mb-10">
            <BackButton to="/admin/auth" />
          </div>
          
          <div className="text-center mb-16">
            <h2 className="text-6xl text-[#1C1C1C] mb-6 font-black" style={{ fontFamily: 'var(--font-head)' }}>
              Admin Sign In
            </h2>
            <p className="text-2xl text-[#6B7280] max-w-2xl mx-auto leading-relaxed">
              Enter your credentials to access the administrative dashboard.
            </p>
          </div>

          <form 
            ref={containerRef}
            onSubmit={handleSubmit}
            onMouseEnter={() => setIsContainerHovered(true)}
            onMouseLeave={() => setIsContainerHovered(false)}
            className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-2xl p-12 relative overflow-hidden shadow-2xl"
            style={{
              borderColor: isContainerHovered ? '#2E5BBA' : '#D1D5DB',
              boxShadow: isContainerHovered 
                ? '0 30px 70px rgba(46, 91, 186, 0.25)'
                : 'none',
            }}
          >
            <div className="space-y-10 relative z-10">
              <div>
                <label className="block text-[#1C1C1C] mb-4 text-xl font-bold">Admin Email</label>
                <input
                  type="email"
                  required
                  value={formData.email}
                  onChange={(e) => setFormData({ ...formData, email: e.target.value })}
                  onMouseEnter={() => setHoveredInput('email')}
                  onMouseLeave={() => setHoveredInput(null)}
                  placeholder="admin@statxtract.com"
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner font-bold"
                  style={{
                    borderColor: hoveredInput === 'email' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredInput === 'email' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-4 text-xl font-bold">Password</label>
                <input
                  type="password"
                  required
                  value={formData.password}
                  onChange={(e) => setFormData({ ...formData, password: e.target.value })}
                  onMouseEnter={() => setHoveredInput('password')}
                  onMouseLeave={() => setHoveredInput(null)}
                  placeholder="••••••••"
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner font-bold"
                  style={{
                    borderColor: hoveredInput === 'password' ? '#2E5BBA' : '#D1D5DB',
                    boxShadow: hoveredInput === 'password' ? '0 0 15px rgba(46, 91, 186, 0.2)' : 'none'
                  }}
                />
              </div>

              <button
                type="submit"
                onMouseEnter={() => setHoveredInput('submit')}
                onMouseLeave={() => setHoveredInput(null)}
                className="lift-on-hover w-full bg-[#2E5BBA] text-white py-5 rounded-xl border-2 cursor-pointer transition-all duration-300 text-2xl font-black shadow-lg mt-4"
                style={{
                  borderColor: hoveredInput === 'submit' ? '#F4A300' : '#2E5BBA',
                  boxShadow: hoveredInput === 'submit' ? '0 15px 40px rgba(244, 163, 0, 0.4)' : 'none'
                }}
              >
                Sign In to Dashboard
              </button>

              <div className="text-center mt-6">
                <p className="text-[#6B7280] text-lg m-0 font-medium">
                  Need a new admin account?{" "}
                  <button 
                    type="button"
                    onClick={() => navigate("/admin/create-account")}
                    className="bg-transparent border-none text-[#2E5BBA] cursor-pointer font-black hover:underline p-0 text-lg"
                  >
                    Create Account
                  </button>
                </p>
              </div>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}
