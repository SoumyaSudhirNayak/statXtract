import { ReactNode, useState, useRef, useEffect } from 'react';

interface HoverGlowProps {
  children: ReactNode;
  glowColor?: string;
  intensity?: 'low' | 'medium' | 'high';
  className?: string;
}

export function HoverGlow({ 
  children, 
  glowColor = 'auto', 
  intensity = 'medium',
  className = '' 
}: HoverGlowProps) {
  const [isHovered, setIsHovered] = useState(false);
  const [mousePos, setMousePos] = useState({ x: 0, y: 0 });
  const elementRef = useRef<HTMLDivElement>(null);

  // Auto-assign glow colors based on context
  const getGlowColor = () => {
    if (glowColor !== 'auto') return glowColor;
    
    // Cycle through different pastel colors
    const colors = [
      'rgba(173, 216, 230, 0.4)', // Soft blue
      'rgba(144, 238, 144, 0.35)', // Soft green
      'rgba(255, 253, 208, 0.45)', // Soft yellow
      'rgba(255, 218, 185, 0.35)', // Soft peach
      'rgba(244, 163, 0, 0.3)',    // Soft saffron
      'rgba(46, 91, 186, 0.3)',    // Government blue
      'rgba(46, 125, 50, 0.3)',    // Government green
    ];
    
    // Use element position to pick a color
    const index = Math.floor(Math.random() * colors.length);
    return colors[index];
  };

  const intensityMap = {
    low: '0 0 20px',
    medium: '0 0 30px',
    high: '0 0 40px',
  };

  const handleMouseMove = (e: React.MouseEvent<HTMLDivElement>) => {
    if (elementRef.current) {
      const rect = elementRef.current.getBoundingClientRect();
      setMousePos({
        x: e.clientX - rect.left,
        y: e.clientY - rect.top,
      });
    }
  };

  const color = getGlowColor();
  
  return (
    <div
      ref={elementRef}
      className={`relative ${className}`}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
      onMouseMove={handleMouseMove}
      style={{
        transition: 'all 0.3s ease',
        boxShadow: isHovered 
          ? `${intensityMap[intensity]} ${color}, 0 4px 20px rgba(0, 0, 0, 0.1)`
          : '0 2px 8px rgba(0, 0, 0, 0.05)',
      }}
    >
      {/* Cursor-following glow overlay */}
      {isHovered && (
        <div
          className="absolute pointer-events-none"
          style={{
            left: `${mousePos.x}px`,
            top: `${mousePos.y}px`,
            width: '150px',
            height: '150px',
            transform: 'translate(-50%, -50%)',
            background: `radial-gradient(circle, ${color}, transparent)`,
            filter: 'blur(20px)',
            opacity: 0.6,
            transition: 'opacity 0.3s ease',
          }}
        />
      )}
      
      {children}
    </div>
  );
}
